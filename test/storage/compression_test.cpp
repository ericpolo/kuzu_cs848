#include "gtest/gtest.h"
#include "storage/store/compression.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::storage;

template<typename T>
void test_compression(CompressionAlg& alg, std::vector<T> src) {
    auto pageSize = 4096;
    std::vector<uint8_t> dest(pageSize);

    auto metadata = alg.getCompressionMetadata((uint8_t*)src.data(), src.size());
    // For simplicity, we'll ignore the possibility of it requiring multiple pages
    // That's tested separately

    auto numValuesRemaining = src.size();
    const uint8_t* srcCursor = (uint8_t*)src.data();
    alg.compressNextPage(srcCursor, numValuesRemaining, dest.data(), pageSize, metadata);
    std::vector<T> decompressed(src.size());
    alg.decompressFromPage(dest.data(), 0, (uint8_t*)decompressed.data(), 0, src.size(), metadata);
    EXPECT_EQ(src, decompressed);
    // works with all bit widths
    T value = 0;
    alg.setValueFromUncompressed((uint8_t*)&value, 0, (uint8_t*)dest.data(), 1, metadata);
    alg.decompressFromPage(dest.data(), 0, (uint8_t*)decompressed.data(), 0, src.size(), metadata);
    src[1] = value;
    EXPECT_EQ(decompressed, src);
    EXPECT_EQ(decompressed[1], value);

    for (int i = 0; i < src.size(); i++) {
        alg.getValue(dest.data(), i, (uint8_t*)decompressed.data(), i, metadata);
        EXPECT_EQ(decompressed[i], src[i]);
    }
    EXPECT_EQ(decompressed, src);

    // Decompress part of a page
    decompressed.clear();
    decompressed.resize(src.size() / 2);
    alg.decompressFromPage(
        dest.data(), src.size() / 3, (uint8_t*)decompressed.data(), 0, src.size() / 2, metadata);
    auto expected = std::vector(src);
    expected.erase(expected.begin(), expected.begin() + src.size() / 3);
    expected.resize(src.size() / 2);
    EXPECT_EQ(decompressed, expected);

    decompressed.clear();
    decompressed.resize(src.size() / 2);
    alg.decompressFromPage(
        dest.data(), src.size() / 7, (uint8_t*)decompressed.data(), 0, src.size() / 2, metadata);
    expected = std::vector(src);
    expected.erase(expected.begin(), expected.begin() + src.size() / 7);
    expected.resize(src.size() / 2);
    EXPECT_EQ(decompressed, expected);
}

TEST(CompressionTests, BooleanBitpackingTest) {
    std::vector<uint8_t> src{true, false, true, true, false, true, false};
    auto alg = BooleanBitpacking();
    test_compression(alg, src);
}

TEST(CompressionTests, UncompressedTest) {
    std::vector<uint8_t> src{true, false, true, true, false, true, false};
    auto alg = Uncompressed(LogicalType(LogicalTypeID::BOOL));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest32) {
    auto length = 128;
    std::vector<int32_t> src(length, 0);
    for (int i = 0; i < length; i++) {
        src[i] = i;
    }
    auto alg = IntegerBitpacking<int32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).bitWidth,
        std::bit_width(static_cast<uint32_t>(length - 1)));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest32Small) {
    auto length = 7;
    std::vector<int32_t> src(length, 0);
    for (int i = 0; i < length; i++) {
        src[i] = i;
    }
    auto alg = IntegerBitpacking<int32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).bitWidth,
        std::bit_width(static_cast<uint32_t>(length - 1)));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest64) {
    std::vector<int64_t> src(128, 6);
    auto alg = IntegerBitpacking<int64_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).bitWidth, std::bit_width(6u));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTestNegative32) {
    std::vector<int32_t> src(128, -6);
    src[5] = 20;
    auto alg = IntegerBitpacking<int32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).bitWidth, std::bit_width(20u) + 1);
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTestNegative64) {
    std::vector<int64_t> src(128, -6);
    src[5] = 20;
    auto alg = IntegerBitpacking<int64_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).bitWidth, std::bit_width(20u) + 1);
    test_compression(alg, src);
}

TEST(CompressionTests, CopyMultiPage) {
    int64_t numValues = 512;
    std::vector<int64_t> src(numValues, -6);

    auto alg = Uncompressed(LogicalType(LogicalTypeID::INT64));
    auto pageSize = 64;
    auto metadata = alg.getCompressionMetadata((uint8_t*)src.data(), src.size());
    auto numValuesRemaining = numValues;
    auto numValuesPerPage = metadata.numValues(pageSize, LogicalType(LogicalTypeID::INT64));
    const uint8_t* srcCursor = (uint8_t*)src.data();
    // TODO: accumulate output and then decompress
    while (numValuesRemaining > 0) {
        std::vector<uint8_t> dest(pageSize);
        auto compressedSize =
            alg.compressNextPage(srcCursor, numValuesRemaining, dest.data(), pageSize, metadata);
        numValuesRemaining -= numValuesPerPage;
        ASSERT_EQ(compressedSize, pageSize);
    }
    ASSERT_EQ((int64_t*)srcCursor - src.data(), numValues);
}

template<typename T>
void integerPackingMultiPage(const std::vector<T>& src) {
    auto alg = IntegerBitpacking<T>();
    auto pageSize = 4096;
    auto metadata = alg.getCompressionMetadata((uint8_t*)src.data(), src.size());
    auto numValuesPerPage = metadata.numValues(pageSize, LogicalType(LogicalTypeID::INT64));
    int64_t numValuesRemaining = src.size();
    const uint8_t* srcCursor = (uint8_t*)src.data();
    auto pages = src.size() / numValuesPerPage + 1;
    std::vector<std::vector<uint8_t>> dest(pages, std::vector<uint8_t>(pageSize));
    size_t pageNum = 0;
    while (numValuesRemaining > 0) {
        ASSERT_LT(pageNum, pages);
        alg.compressNextPage(
            srcCursor, numValuesRemaining, dest[pageNum++].data(), pageSize, metadata);
        numValuesRemaining -= numValuesPerPage;
    }
    ASSERT_EQ(srcCursor, (uint8_t*)(src.data() + src.size()));
    for (int i = 0; i < src.size(); i++) {
        auto page = i / numValuesPerPage;
        auto indexInPage = i % numValuesPerPage;
        T value;
        alg.getValue(dest[page].data(), indexInPage, (uint8_t*)&value, 0, metadata);
        EXPECT_EQ(src[i] - value, 0);
        EXPECT_EQ(src[i], value);
    }
    std::vector<T> decompressed(src.size());
    for (int i = 0; i < src.size(); i += numValuesPerPage) {
        auto page = i / numValuesPerPage;
        alg.decompressFromPage(dest[page].data(), 0, (uint8_t*)decompressed.data(), i,
            std::min(numValuesPerPage, (uint64_t)src.size() - i), metadata);
    }
    ASSERT_EQ(decompressed, src);
}

TEST(CompressionTests, IntegerPackingMultiPage64) {
    int64_t numValues = 10000;
    std::vector<int64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageNegative64) {
    int64_t numValues = 10000;
    std::vector<int64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = -i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPage32) {
    int64_t numValues = 10000;
    std::vector<int32_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageNegative32) {
    int64_t numValues = 10000;
    std::vector<int32_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = -i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageUnsigned32) {
    int64_t numValues = 10000;
    std::vector<uint32_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}

TEST(CompressionTests, IntegerPackingMultiPageUnsigned64) {
    int64_t numValues = 10000;
    std::vector<uint64_t> src(numValues);
    for (int i = 0; i < numValues; i++) {
        src[i] = i;
    }

    integerPackingMultiPage(src);
}