#pragma once

#include <cstring>

#include "common/file_utils.h"
#include "gtest/gtest.h"
#include "main/kuzu.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "test_helper/test_helper.h"

using namespace kuzu::main;
using ::testing::Test;

namespace kuzu {
namespace testing {

enum class TransactionTestType : uint8_t {
    NORMAL_EXECUTION = 0,
    RECOVERY = 1,
};

class BaseGraphTest : public Test {
public:
    void SetUp() override {
        systemConfig =
            make_unique<SystemConfig>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        if (FileUtils::fileOrPathExists(TestHelper::getTmpTestDir())) {
            FileUtils::removeDir(TestHelper::getTmpTestDir());
        }
        databaseConfig = make_unique<DatabaseConfig>(TestHelper::getTmpTestDir());
    }

    virtual string getInputDir() = 0;

    void TearDown() override { FileUtils::removeDir(TestHelper::getTmpTestDir()); }

    inline void createDBAndConn() {
        if (database != nullptr) {
            database.reset();
        }
        database = make_unique<Database>(*databaseConfig, *systemConfig);
        conn = make_unique<Connection>(database.get());
        spdlog::set_level(spdlog::level::info);
    }

    void initGraph();

    void initGraphFromPath(const string& path) const;

    void commitOrRollbackConnection(bool isCommit, TransactionTestType transactionTestType) const;

protected:
    // Static functions to access Database's non-public properties/interfaces.
    static inline catalog::Catalog* getCatalog(Database& database) {
        return database.catalog.get();
    }
    static inline storage::StorageManager* getStorageManager(Database& database) {
        return database.storageManager.get();
    }
    static inline storage::BufferManager* getBufferManager(Database& database) {
        return database.bufferManager.get();
    }
    static inline storage::MemoryManager* getMemoryManager(Database& database) {
        return database.memoryManager.get();
    }
    static inline transaction::TransactionManager* getTransactionManager(Database& database) {
        return database.transactionManager.get();
    }
    static inline uint64_t getDefaultBMSize(Database& database) {
        return database.systemConfig.defaultPageBufferPoolSize;
    }
    static inline uint64_t getLargeBMSize(Database& database) {
        return database.systemConfig.largePageBufferPoolSize;
    }
    static inline WAL* getWAL(Database& database) { return database.wal.get(); }
    static inline void commitAndCheckpointOrRollback(Database& database,
        transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false) {
        database.commitAndCheckpointOrRollback(
            writeTransaction, isCommit, skipCheckpointForTestingRecovery);
    }
    static inline QueryProcessor* getQueryProcessor(Database& database) {
        return database.queryProcessor.get();
    }

    // Static functions to access Connection's non-public properties/interfaces.
    static inline Connection::ConnectionTransactionMode getTransactionMode(Connection& connection) {
        return connection.getTransactionMode();
    }
    static inline void setTransactionModeNoLock(
        Connection& connection, Connection::ConnectionTransactionMode newTransactionMode) {
        connection.setTransactionModeNoLock(newTransactionMode);
    }
    static inline void commitButSkipCheckpointingForTestingRecovery(Connection& connection) {
        connection.commitButSkipCheckpointingForTestingRecovery();
    }
    static inline void rollbackButSkipCheckpointingForTestingRecovery(Connection& connection) {
        connection.rollbackButSkipCheckpointingForTestingRecovery();
    }
    static inline Transaction* getActiveTransaction(Connection& connection) {
        return connection.getActiveTransaction();
    }
    static inline uint64_t getMaxNumThreadForExec(Connection& connection) {
        return connection.getMaxNumThreadForExec();
    }
    static inline uint64_t getActiveTransactionID(Connection& connection) {
        return connection.getActiveTransactionID();
    }
    static inline bool hasActiveTransaction(Connection& connection) {
        return connection.hasActiveTransaction();
    }
    static inline void commitNoLock(Connection& connection) { connection.commitNoLock(); }
    static inline void rollbackIfNecessaryNoLock(Connection& connection) {
        connection.rollbackIfNecessaryNoLock();
    }
    static inline void sortAndCheckTestResults(
        vector<string>& actualResult, vector<string>& expectedResult) {
        sort(expectedResult.begin(), expectedResult.end());
        ASSERT_EQ(actualResult, expectedResult);
    }
    static inline bool containsOverflowFile(DataTypeID typeID) {
        return typeID == STRING || typeID == LIST;
    }

    void validateColumnFilesExistence(string fileName, bool existence, bool hasOverflow);

    void validateListFilesExistence(
        string fileName, bool existence, bool hasOverflow, bool hasHeader);

    void validateNodeColumnFilesExistence(
        NodeTableSchema* nodeTableSchema, DBFileType dbFileType, bool existence);

    void validateRelColumnAndListFilesExistence(
        RelTableSchema* relTableSchema, DBFileType dbFileType, bool existence);

    void validateQueryBestPlanJoinOrder(string query, string expectedJoinOrder);

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType);

private:
    void validateRelPropertyFiles(catalog::RelTableSchema* relTableSchema, table_id_t tableID,
        RelDirection relDirection, bool isColumnProperty, DBFileType dbFileType, bool existence);

public:
    unique_ptr<SystemConfig> systemConfig;
    unique_ptr<DatabaseConfig> databaseConfig;
    unique_ptr<Database> database;
    unique_ptr<Connection> conn;
};

// This class starts database without initializing graph.
class EmptyDBTest : public BaseGraphTest {
    string getInputDir() override { throw NotImplementedException("getInputDir()"); }
};

// This class starts database in on-disk mode.
class DBTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }

    inline void runTest(const string& queryFile) {
        auto queryConfigs = TestHelper::parseTestFile(queryFile);
        ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
    }

    inline void runTestAndCheckOrder(const string& queryFile) {
        auto queryConfigs = TestHelper::parseTestFile(queryFile, true /* checkOutputOrder */);
        for (auto& queryConfig : queryConfigs) {
            queryConfig->checkOutputOrder = true;
        }
        ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
    }
};

} // namespace testing
} // namespace kuzu