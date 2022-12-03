#include "test_helper/test_helper.h"

using ::testing::Test;
using namespace kuzu::testing;

class LongStringPKTest : public DBTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/long-string-pk-tests/");
    }
};

class TinySnbReadTest : public DBTest {
public:
    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }
};

TEST_F(LongStringPKTest, LongStringPKTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/long_string_pk/long_string_pk.test"));
}

TEST_F(TinySnbReadTest, MatchExecute) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/node.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/one_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/two_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/three_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/four_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/multi_query_part.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/match/multi_label.test"));
}

TEST_F(TinySnbReadTest, Filter) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/node.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/one_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/two_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/four_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/five_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/multi_query_part.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/filter/multi_label.test"));
}

TEST_F(TinySnbReadTest, Function) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/date.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/timestamp.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/interval.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/list.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/arithmetic.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/boolean.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/string.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/function/cast.test"));
}

TEST_F(TinySnbReadTest, Agg) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/agg/simple.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/agg/hash.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/agg/distinct_agg.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/agg/multi_query_part.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/agg/multi_label.test"));
}

TEST_F(TinySnbReadTest, Cyclic) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/cyclic/cyclic.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/cyclic/multi_label.test"));
}

TEST_F(TinySnbReadTest, Projection) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/projection/projection.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/projection/skip_limit.test"));
    runTest(
        TestHelper::appendKuzuRootPath("test/test_files/tinysnb/projection/multi_query_part.test"));
}

TEST_F(TinySnbReadTest, Subquery) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/subquery/exists.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/subquery/multi_label.test"));
}

TEST_F(TinySnbReadTest, OptionalMatch) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/tinysnb/optional_match/optional_match.test"));
    runTest(
        TestHelper::appendKuzuRootPath("test/test_files/tinysnb/optional_match/multi_label.test"));
}

TEST_F(TinySnbReadTest, OrderBy) {
    auto queryConfigs = TestHelper::parseTestFile(
        TestHelper::appendKuzuRootPath("test/test_files/tinysnb/order_by/order_by.test"),
        true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig->checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}

TEST_F(TinySnbReadTest, Union) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/union/union.test"));
}

TEST_F(TinySnbReadTest, Unwind) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinysnb/unwind/unwind.test"));
}

TEST_F(TinySnbReadTest, VarLengthExtendTests) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/tinysnb/var_length_extend/var_length_adj_list_extend.test"));
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/tinysnb/var_length_extend/var_length_column_extend.test"));
}
