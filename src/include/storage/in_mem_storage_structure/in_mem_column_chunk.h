#pragma once

#include "common/types/types.h"
#include "function/cast/numeric_cast.h"
#include "storage/storage_structure/in_mem_file.h"
#include "storage/store/table_copy_utils.h"
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>

namespace kuzu {
namespace storage {

struct PropertyCopyState {
    PageByteCursor overflowCursor;
};

class InMemColumnChunk {
public:
    InMemColumnChunk(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, std::unique_ptr<common::CSVReaderConfig> csvReaderConfig,
        bool requireNullBits = true);

    virtual ~InMemColumnChunk() = default;

    inline common::LogicalType getDataType() const { return dataType; }

    template<typename T>
    inline T getValue(common::offset_t pos) const {
        return ((T*)buffer.get())[pos];
    }

    void setValueAtPos(const uint8_t* val, common::offset_t pos);

    inline bool isNull(common::offset_t pos) const {
        assert(nullChunk);
        return nullChunk->getValue<bool>(pos);
    }
    inline uint8_t* getData() const { return buffer.get(); }
    inline uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    inline uint64_t getNumBytes() const { return numBytes; }
    inline InMemColumnChunk* getNullChunk() { return nullChunk.get(); }
    void copyArrowBatch(std::shared_ptr<arrow::RecordBatch> batch);
    virtual void copyArrowArray(arrow::Array& arrowArray, PropertyCopyState* copyState,
        arrow::Array* nodeOffsets = nullptr);
    virtual void flush(common::FileInfo* walFileInfo);

    template<typename T>
    void templateCopyValuesToPage(arrow::Array& array, arrow::Array* nodeOffsets);

    template<typename T, typename... Args>
    void setValueFromString(
        const char* value, uint64_t length, common::offset_t pos, Args... args) {
        auto val = function::castStringToNum<T>(value, length);
        setValue(val, pos);
    }

    template<typename T>
    inline void setValue(T val, common::offset_t pos) {
        ((T*)buffer.get())[pos] = val;
    }

private:
    template<typename ARROW_TYPE>
    void templateCopyArrowStringArray(arrow::Array& array, arrow::Array* nodeOffsets);

    template<typename KU_TYPE, typename ARROW_TYPE>
    void templateCopyValuesAsStringToPage(arrow::Array& array, arrow::Array* nodeOffsets);

    inline virtual common::offset_t getOffsetInBuffer(common::offset_t pos) {
        return pos * numBytesPerValue;
    }

    static uint32_t getDataTypeSizeInColumn(common::LogicalType& dataType);

protected:
    common::LogicalType dataType;
    common::offset_t startNodeOffset;
    std::uint64_t numBytesPerValue;
    std::uint64_t numBytes;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<InMemColumnChunk> nullChunk;
    std::unique_ptr<common::CSVReaderConfig> csvReaderConfig;
};

class InMemColumnChunkWithOverflow : public InMemColumnChunk {
public:
    InMemColumnChunkWithOverflow(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, std::unique_ptr<common::CSVReaderConfig> csvReaderConfig,
        InMemOverflowFile* inMemOverflowFile)
        : InMemColumnChunk{std::move(dataType), startNodeOffset, endNodeOffset,
              std::move(csvReaderConfig)},
          inMemOverflowFile{inMemOverflowFile}, blobBuffer{std::make_unique<uint8_t[]>(
                                                    common::BufferPoolConstants::PAGE_4KB_SIZE)} {}

    void copyArrowArray(arrow::Array& array, PropertyCopyState* copyState,
        arrow::Array* nodeOffsets = nullptr) final;

    void copyValuesToPageWithOverflow(
        arrow::Array& array, PropertyCopyState* copyState, arrow::Array* nodeOffsets);

    template<typename T>
    void setValWithOverflow(
        PageByteCursor& overflowCursor, const char* value, uint64_t length, uint64_t pos) {
        assert(false);
    }

private:
    template<typename KU_TYPE>
    void templateCopyArrowStringArray(
        arrow::Array& array, PropertyCopyState* copyState, arrow::Array* nodeOffsets);

    template<typename KU_TYPE, typename ARROW_TYPE>
    void templateCopyValuesAsStringToPageWithOverflow(
        arrow::Array& array, PropertyCopyState* copyState, arrow::Array* nodeOffsets);

private:
    storage::InMemOverflowFile* inMemOverflowFile;
    std::unique_ptr<uint8_t[]> blobBuffer;
};

class InMemFixedListColumnChunk : public InMemColumnChunk {
public:
    InMemFixedListColumnChunk(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, std::unique_ptr<common::CSVReaderConfig> csvReaderConfig);

    void flush(common::FileInfo* walFileInfo) override;

private:
    common::offset_t getOffsetInBuffer(common::offset_t pos) override;

private:
    uint64_t numElementsInAPage;
};

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(arrow::Array& array, arrow::Array* offsets);
template<>
void InMemColumnChunk::templateCopyValuesToPage<uint8_t*>(
    arrow::Array& array, arrow::Array* offsets);

// BOOL
template<>
void InMemColumnChunk::setValueFromString<bool>(
    const char* value, uint64_t length, common::offset_t pos);
// FIXED_LIST
template<>
void InMemColumnChunk::setValueFromString<uint8_t*>(
    const char* value, uint64_t length, uint64_t pos);
// INTERVAL
template<>
void InMemColumnChunk::setValueFromString<common::interval_t>(
    const char* value, uint64_t length, uint64_t pos);
// DATE
template<>
void InMemColumnChunk::setValueFromString<common::date_t>(
    const char* value, uint64_t length, uint64_t pos);
// TIMESTAMP
template<>
void InMemColumnChunk::setValueFromString<common::timestamp_t>(
    const char* value, uint64_t length, uint64_t pos);

} // namespace storage
} // namespace kuzu
