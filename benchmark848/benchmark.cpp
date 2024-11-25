#include <iostream>
#include <string>
#include <chrono>
#include <algorithm>
#include <sys/stat.h>
#include <cstdlib>
#include <stdlib.h>
#include <ctime>
#include <fstream>
#include <sstream>

#ifdef DEBUG
#include "../build/debug/src/kuzu.hpp"
#else
#include "../build/release/src/kuzu.hpp"
#endif

using namespace kuzu::main;
using namespace std;
using namespace std::chrono;

/* We create three node tables: People, Customer, and Organization. All three tables contain 100k entries */
const int NUM_TABLES = 3;
const int NUM_ROWS = 100000;
const string tableNames[NUM_TABLES] = {"People", "Customer", "Organization"};
const string tableCreateQuery [NUM_TABLES] = {
    "CREATE NODE TABLE People (id INT32, firstName STRING, lastName STRING, sex STRING, email STRING, phone STRING, jobTitle STRING, PRIMARY KEY(id));",
    "CREATE NODE TABLE Customer (id INT32, firstName STRING, lastName STRING, company STRING, city STRING, country STRING, primaryPhone STRING, secondaryPhone STRING, email STRING, website STRING, PRIMARY KEY(id));",
    "CREATE NODE TABLE Organization (id INT32, name STRING, website STRING, country STRING, description STRING, foundYear INT16, industry STRING, numEmployee INT16, PRIMARY KEY(id));"
};

const vector<string> tableColumns[NUM_TABLES] = {{"id", "firstName", "lastName", "sex", "email", "phone", "jobTitle"},
    {"id", "firstName", "lastName", "company", "city", "country", "primaryPhone", "secondaryPhone", "email", "website"},
    {"id", "name", "website", "country", "description", "foundYear", "industry", "numEmployee"}};
const int tableNumColumns[3] = {7, 10, 8};

/* These variables will be updated later after user input */
string tableCopyQuery [NUM_TABLES] = {"", "", ""};
string databaseHomeDirectory = "";
string dataFilePath = "";
string metadataFilePath = "";

enum class AlterType
{
    DROP_COLUMN,
    ADD_COLUMN,
    INVALID_ALTER_OPERATION
};

enum class AlterAddColumnType
{
    INVALID_COLUMN_TYPE,
    INT64,
    BOOL,
    STRING
};

enum class TestType
{
    DROP_TABLE,
    ALTER_TABLE,
    DELETE_NODE_GROUP,
    INVALID_TEST
};

enum Strategy : int {
    AUTO,
    FIXED,
    ROUND_ROBIN,
    SEED,
    INVALID_STRATEGY
};

/* Operator overloading for output string */
std::ostream& operator<<(std::ostream& os, AlterType alterType) {
    switch (alterType) {
        case AlterType::DROP_COLUMN:
            os << "DROP COLUMN ";
            break;
        case AlterType::ADD_COLUMN:
            os << "ADD COLUMN ";
            break;
        default:
            os << "INVALID_ALTER_OPERATION";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, AlterAddColumnType alterAddColumnType) {
    switch (alterAddColumnType) {
        case AlterAddColumnType::INT64:
            os << "INT64 ";
            break;
        case AlterAddColumnType::BOOL:
            os << "BOOL ";
            break;
        case AlterAddColumnType::STRING:
            os << "STRING ";
            break;
        default: os << "INVALID_COLUMN_TYPE";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, TestType testType) {
    switch (testType) {
        case TestType::DROP_TABLE:
            os << "DROP TABLE ";
            break;
        case TestType::ALTER_TABLE:
            os << "ALTER TABLE ";
            break;
        case TestType::DELETE_NODE_GROUP:
            os << "DELETE NODE_GROUP ";
            break;
        default:
            os << "INVALID_TEST";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, Strategy strategy) {
    switch (strategy) {
        case Strategy::AUTO:
            os << "AUTO";
            break;
        case Strategy::FIXED:
            os << "FIXED";
            break;
        case Strategy::ROUND_ROBIN:
            os << "ROUND_ROBIN";
            break;
        case Strategy::SEED:
            os << "SEED";
            break;
        default:
            os << "UNKNOWN";
            break;
    }
    return os;
}

Strategy parseStrategy(const std::string& str) {
    static const std::unordered_map<std::string, Strategy> strategyMap = {
        {"auto", Strategy::AUTO},
        {"fixed", Strategy::FIXED},
        {"round", Strategy::ROUND_ROBIN},
        {"seed", Strategy::SEED}
    };

    auto it = strategyMap.find(str);
    if (it != strategyMap.end()) {
        return it->second;
    } else {
        return Strategy::INVALID_STRATEGY;
    }
}

/* This struct keep tracks of stat of each test case */
typedef struct TestCaseStat {
    /* Test metadata */
    string testName = "";
    string tableName = "";
    string columnName = "";
    int recordsDeleted = 0;

    /* Accumulated Checkpoint time */
    microseconds checkPointTimeAcc;
    /* Number of Checkpoint executed */
    int numCheckPoint;
    /* Duration of current test case */
    microseconds runningDuration;
    /* Data file size */
    long dataFileSize;
    /* Metadata file size */
    long metadataFileSize;

    void PrintStat() {
        cout << "Current test case stat:" << endl
             << "    Test name:                " << testName << endl
             << "    Table name:               " << tableName << endl
             << "    Column name:              " << columnName << endl
             << "    Records deleted:          " << recordsDeleted << endl
             << "    checkPointTimeAccumulate: " << checkPointTimeAcc.count() <<" μs"<< endl
             << "    checkPointCounts:         " << numCheckPoint << endl
             << "    checkPointTimeAverage:    " << checkPointTimeAcc.count() / numCheckPoint << " μs" << endl
             << "    runningDuration:          " << runningDuration.count() << " μs" << endl
             << "    dataFileSize:             " << dataFileSize << " bytes" << endl
             << "    metadataFileSize:         " << metadataFileSize << " bytes" << endl;
    }

    static void PrintAllStat(vector<TestCaseStat> allStat) {
        TestCaseStat accStat = {"", "", "", 0, microseconds(0), 0, microseconds(0), 0, 0};

        for (auto stat : allStat) {
            accStat.recordsDeleted += stat.recordsDeleted;
            accStat.checkPointTimeAcc += stat.checkPointTimeAcc;
            accStat.numCheckPoint += stat.numCheckPoint;
            accStat.runningDuration += stat.runningDuration;
            accStat.dataFileSize += stat.dataFileSize;
            accStat.metadataFileSize += stat.metadataFileSize;
        }
        cout << "Overall test cases stat:" << endl
             << "    totalRecordsDeleted:      " << accStat.recordsDeleted << endl
             << "    checkPointTimeAccumulate: " << accStat.checkPointTimeAcc.count() <<" μs"<< endl
             << "    checkPointCounts:         " << accStat.numCheckPoint << endl
             << "    checkPointTimeAverage:    " << accStat.checkPointTimeAcc.count() / accStat.numCheckPoint << " μs" << endl
             << "    runningDuration:          " << accStat.runningDuration.count() << " μs" << endl
             << "    dataFileSizeAvg:          " << accStat.dataFileSize / allStat.size() << " bytes" << endl
             << "    dataFileSizeFinal:        " << allStat.back().dataFileSize << " bytes" << endl
             << "    metadataFileSizeAvg:      " << allStat.back().metadataFileSize << " bytes" << endl
             << "    metadataFileSizeFinal:    " << allStat.back().metadataFileSize << " bytes" << endl;
    }

    static void publishCsv(vector<TestCaseStat> allStat, string fileName) {
        std::ofstream csvFile(fileName);

        // print csv headers
        csvFile << "Test Name,Table Name,Column Name,Records Deleted,Checkpoint time,Num Checkpoints,"
                << "Running Duration,Data File Size,Metadata File Size" << endl;
        for (auto stat : allStat) {
            // Print exact values
            csvFile << stat.testName << ","
                    << stat.tableName << ","
                    << stat.columnName << ","
                    << stat.recordsDeleted << ","
                    << stat.checkPointTimeAcc.count() << ","
                    << stat.numCheckPoint << ","
                    << stat.runningDuration.count() << ","
                    << stat.dataFileSize << ","
                    << stat.metadataFileSize << endl;
        }
        csvFile.close();
    }
} TestCaseStat;

/* Miscellaneous Helper functions */
string GenRandomStr(const int len)
{
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    
    return tmp_s;
}

char* GetCmdOption(char ** begin, char ** end, const string & option)
{
    char ** itr = find(begin, end, option);
    if (itr != end && ++itr != end)
    {
        return *itr;
    }
    return nullptr;
}

void UpdateTableCopyQuery(const string& csvFileDir)
{
    tableCopyQuery[0] = "COPY People FROM '" + csvFileDir + +"/people-100000.csv';";
    tableCopyQuery[1] = "COPY Customer FROM '" + csvFileDir + +"/customers-100000.csv';";
    tableCopyQuery[2] = "COPY Organization FROM '" + csvFileDir + +"/organizations-100000.csv';";
}

long GetFileSize(const string& filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

string RandomSelectTable(const string& avoidTable)
{
    string tableName = avoidTable;
    do {
        int tableIndex = rand() % NUM_TABLES;
        tableName = tableNames[tableIndex];
    } while (tableName == avoidTable);

    return tableName;
}

int FindTableIndex(const string& tableName)
{
    for (int i = 0; i < NUM_TABLES; i++) {
        if (tableNames[i] == tableName) {
            return i;
        }
    }

    KU_ASSERT(0);
    return 0;
}

/* Main Helper functions used by test case */
void CreateTable(const unique_ptr<Connection> &connection, string tableName)
{
    int index = FindTableIndex(tableName);
    KU_ASSERT(index < NUM_TABLES);
    connection->query(tableCreateQuery[index]);
    connection->query(tableCopyQuery[index]);
}

void DropTable(const unique_ptr<Connection> &connection, string tableName)
{
    string query = "DROP TABLE " + tableName + ";";
    connection->query(query);
}

/* Note that colType is only valid when alterType is ADD_COLUMN. Otherwise, it should be set as INVALID_COLUMN_TYPE */
void AlterTable(const unique_ptr<Connection> &connection, string tableName, AlterType alterType,
    string colName, AlterAddColumnType colType = AlterAddColumnType::INVALID_COLUMN_TYPE)
{
    KU_ASSERT(alterType != AlterType::ADD_COLUMN || colType == AlterAddColumnType::INVALID_COLUMN_TYPE);

    /* define base query here */
    string query = "ALTER TABLE" + tableName;
    switch (alterType) {
        case AlterType::DROP_COLUMN:
            query += (" DROP COLUMN" + colName);
            break;
        case AlterType::ADD_COLUMN:
            query += (" ADD COLUMN" + colName);
            switch (colType) {
                case AlterAddColumnType::INT64: {
                    int defaultVal = rand();
                    query += (" DEFAULT " + to_string(defaultVal));
                    break;
                }
                case AlterAddColumnType::BOOL: {
                    int defaultVal = rand() % 2;
                    string defaultStr = (defaultVal == 1)? "True":"False";
                    query += (" DEFAULT " + defaultStr);
                    break;
                }
                case AlterAddColumnType::STRING: {
                    string defaultStr = GenRandomStr(20);
                    query += (" DEFAULT " + defaultStr);
                    break;
                }
                default:
                    cout << "BENCHMARK: Unexpected column type:" << colType << endl;
                    KU_ASSERT(0);
            }
            break;
        default:
            cout << "BENCHMARK: Unexpected Alter type: " << alterType << endl;
            KU_ASSERT(0);
    }
    connection->query(query);
}

microseconds Checkpoint(const unique_ptr<Connection> &connection)
{
    auto start = high_resolution_clock::now();
    connection->query("CHECKPOINT;");
    auto end = high_resolution_clock::now();

    return duration_cast<microseconds>(end - start);
}

void DeleteEntries(const unique_ptr<Connection> &connection, string tableName, int beginId, int endId)
{
    KU_ASSERT(beginId <= endId && beginId < NUM_ROWS && endId < NUM_ROWS);
    string query = "MATCH (entity:" + tableName + "})"
        "WHERE entity.id >" + to_string(beginId) + " AND entity.id < " + to_string(endId) +" "
        "DELETE entity RETURN entity.*;";
    connection->query(query);
}

/* Test functions */
/* Drop Table Test */
void DropTableTest(const unique_ptr<Connection> &connection, TestCaseStat &stat)
{
    string tableName = RandomSelectTable("");
    string nextTableName = RandomSelectTable(tableName);
    auto start = high_resolution_clock::now();
    microseconds ckptAccTime = microseconds(0);

    /*
     * 1. Create table here first
     *      Checkpoint will create physical storage
     */
    CreateTable(connection, tableName);
    ckptAccTime += Checkpoint(connection);

    /*
     * 2. Drop the table and Create another table.
     *      Dropped chunks are recycled in DROP.
     *      Checkpoint will create physical storage for new table
     */
    DropTable(connection, tableName);
    CreateTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 3. Update TestCaseStat here */
    stat.runningDuration = duration_cast<microseconds>(high_resolution_clock::now() - start);
    stat.testName = "DropTableTest";
    stat.tableName = tableName;
    stat.checkPointTimeAcc = ckptAccTime;
    stat.numCheckPoint = 2;
    stat.dataFileSize = GetFileSize(dataFilePath);
    stat.metadataFileSize = GetFileSize(metadataFilePath);

    /* 4. Drop the second tables here to wrap up the test. Do not consider metrics form here */
    DropTable(connection, nextTableName);
    Checkpoint(connection);
}

/*
 * Alter Table Test
 * This test is limited to test Alter Table DROP ... for now
 */
void AlterTableTest(const unique_ptr<Connection> &connection, TestCaseStat &stat)
{
    string tableName = RandomSelectTable("");
    int tableIndex = FindTableIndex(tableName);
    string nextTableName = RandomSelectTable(tableName);
    auto start = high_resolution_clock::now();
    microseconds ckptAccTime = microseconds(0);

    /*
     * 1. Create table here first
     *      Checkpoint will create physical storage
     */
    CreateTable(connection, tableName);
    ckptAccTime += Checkpoint(connection);

    /*
     * 2. Alter the table and Create another table.
     *      chunks are recycled in ALTER.
     *      Checkpoint will create physical storage for new table
     */
    int numColumn = tableNumColumns[tableIndex];
    int droppedColIndex = rand() % numColumn;
    string dropColName = tableColumns[tableIndex][droppedColIndex];
    AlterTable(connection, tableName, AlterType::DROP_COLUMN, dropColName);
    CreateTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 3. Update TestCaseStat here */
    stat.runningDuration = duration_cast<microseconds>(high_resolution_clock::now() - start);
    stat.testName = "AlterTableTest";
    stat.tableName = tableName;
    stat.columnName = dropColName;
    stat.checkPointTimeAcc = ckptAccTime;
    stat.numCheckPoint = 2;
    stat.dataFileSize = GetFileSize(dataFilePath);
    stat.metadataFileSize = GetFileSize(metadataFilePath);

    /* 4. Drop both tables here to wrap up the test. Do not consider metrics from here */
    DropTable(connection, tableName);
    DropTable(connection, nextTableName);
    Checkpoint(connection);
}

/* Delete Node Group Test */
void DeleteNodeGroupTest(const unique_ptr<Connection> &connection, TestCaseStat &stat)
{
    string tableName = RandomSelectTable("");
    string nextTableName = RandomSelectTable(tableName);
    auto start = high_resolution_clock::now();
    microseconds ckptAccTime = microseconds(0);

    /*
     * 1. Create table here first
     *      Checkpoint will create physical storage
     */
    CreateTable(connection, tableName);
    ckptAccTime += Checkpoint(connection);

    /*
     * 2. Delete table entries based on random ratio
     *      chunks are recycled in Checkpoint.
     */
    int beginId = (rand() % 50) * NUM_ROWS / 100;
    int endId = (rand() % 50 + 50) * NUM_ROWS / 100;
    DeleteEntries(connection, tableName, beginId, endId);
    ckptAccTime += Checkpoint(connection);

    /*
     * 3. Create a new table to resue recycled spaces
     */
    CreateTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 4. Update TestCaseStat here */
    stat.runningDuration = duration_cast<microseconds>(high_resolution_clock::now() - start);
    stat.testName = "DeleteNodeGroupTest";
    stat.tableName = tableName;
    stat.recordsDeleted = endId - beginId;
    stat.checkPointTimeAcc = ckptAccTime;
    stat.numCheckPoint = 3;
    stat.dataFileSize = GetFileSize(dataFilePath);
    stat.metadataFileSize = GetFileSize(metadataFilePath);

    /* 5. Drop both tables here to wrap up the test. Do not consider metrics from here */
    DropTable(connection, tableName);
    DropTable(connection, nextTableName);
    Checkpoint(connection);
}

TestType getTestCaseByStrategy(Strategy strategy, int& value) {
    switch (strategy) {
        case Strategy::AUTO:
        case Strategy::SEED:
            return static_cast<TestType>(rand() % 3);
        case Strategy::ROUND_ROBIN:
            return static_cast<TestType>(value++ % 3);
        case Strategy::FIXED:
            return static_cast<TestType>(value);
        default: return TestType::INVALID_TEST;
    }
}

/* Main function for the benchmark */
int main(int argc, char* argv[])
{
    if (GetCmdOption(argv, argv + argc, "-h") != nullptr || argc < 9) {
        cout << "Please provide following parameters:" << endl
             << "    -N <number of iteration you want to run>" << endl
             << "    -D <directory of the csv source files>" << endl
             << "    -B <directory of database>" << endl
             << "    -S <strategy> one of auto | fixed | round | seed>" << endl
             << "    -V <value> Specify the value for the strategy\n"
             << "               If strategy is auto, value is ignored.\n"
             << "               If strategy is fixed or round, value may be one of:\n"
             << "                  0. DROP_TABLE\n"
             << "                  1. DELETE_NODE_GROUP\n"
             << "                  2. ALTER_TABLE\n"
             << "               If strategy is seed, value can be any integer." << endl;
        return 0;
    }

    /* parse the argument first */
    char *csvFileDir = GetCmdOption(argv, argv + argc, "-D");
    if (csvFileDir == nullptr) {
        cout << "Please use -D to specify the directory that saves the csv source file" << endl
             << "Please use -h option to see what parameter we need" << endl;
        return 0;
    }
    UpdateTableCopyQuery(csvFileDir);

    int maxIteration = 0;
    char *iterationsArg = GetCmdOption(argv, argv + argc, "-N");
    if (iterationsArg == 0) {
        cout << "Please use -N to specify the directory that saves the csv source files" << endl
             << "Please use -h option to see what parameter we need" << endl;
        return 0;
    }
    maxIteration = atoi(iterationsArg);

    char *databaseDir = GetCmdOption(argv, argv + argc, "-B");
    if (databaseDir == nullptr) {
        cout << "Please use -B to specify the directory that saves database files" << endl
             << "Please use -h option to see what parameter we need" << endl;
        return 0;
    }

    char* strategyStr = GetCmdOption(argv, argv + argc, "-S");
    if (strategyStr == nullptr) {
        cout << "Please use -S to specify the strategy to be used for testing" << endl
             << "Please use -h option to see what parameter we need" << endl;
        return 0;
    }
    Strategy strategy = parseStrategy(strategyStr);
    if (strategy == Strategy::INVALID_STRATEGY) {
        cout << strategyStr << " is not a valid strategy" << endl
             << "Please use -h option to see what parameter we need" << endl;
        return 0;
    }

    int value = -1;
    if (strategy != Strategy::AUTO) {
        char* valueArg = GetCmdOption(argv, argv + argc, "-V");
        if (valueArg == nullptr) {
            cout << "Please use -V to specify the value to use for strategy " << strategyStr << endl
                 << "Please use -h option to see what parameter we need" << endl;
            return 0;
        }
        value = atoi(valueArg);
        if ((strategy == Strategy::FIXED || strategy == Strategy::ROUND_ROBIN) &&
            (value < 0 || value > 2)) {
            cout << valueArg << " is not a valid value for strategy " << strategyStr << endl
                 << "Please use -h option to see what parameter we need" << endl;
            return 0;
        }
    }

    databaseHomeDirectory = databaseDir;
    dataFilePath = databaseHomeDirectory + "/data.kz";
    metadataFilePath = databaseHomeDirectory + "/metadata.kz";

    cout<<"User Parameters:"<<endl
        <<"    Csv file source directory: "<<csvFileDir<<endl
        <<"    Max test iterations: "<<maxIteration<<endl
        <<"    Database home directory: "<<databaseDir<<endl
        <<"    Test strategy: "<<strategyStr<<endl
        <<"    Strategy value: "<<value<<endl
        <<"    Debug Build?: "
#ifdef DEBUG
        <<"True"
#else
        <<"False"
#endif
        <<endl;

    /* Seed random generator before proceed */
    if (strategy == Strategy::SEED) {
        srand(static_cast<unsigned int>(value));
    } else if (strategy == Strategy::AUTO) {
        srand(static_cast<unsigned int>(time(0)));
    } else {
        srand(123u); // Guarantee of same tables and columns for fixed tests
    }
    int valueCopy = value; // round-robin modifies the original value to keep track

    /* Create an empty on-disk database and connect to it */
    kuzu::main::SystemConfig systemConfig;
    auto database = make_unique<Database>(databaseDir, systemConfig);
    auto connection = make_unique<Connection>(database.get());

    if (connection != nullptr) {
        int curIter = 1;
        vector<TestCaseStat> allStat = {};
        while (curIter <= maxIteration) {
            cout << "----------------------------\nBegin " + to_string(curIter) + "th iterations\n----------------------------" << endl;
            TestType testCase = getTestCaseByStrategy(strategy, valueCopy);
            TestCaseStat stat = {"", "", "", 0, microseconds(0), 0, microseconds(0), 0, 0};
            switch (testCase) {
                case TestType::DROP_TABLE: {
                    cout << "Test Type: DROP_TABLE" << endl;
                    DropTableTest(connection, stat);
                    break;
                }
                case TestType::ALTER_TABLE: {
                    cout << "Test Type: ALTER_TABLE" << endl;
                    AlterTableTest(connection, stat);
                    break;
                }
                case TestType::DELETE_NODE_GROUP: {
                    cout << "Test Type: DELETE_NODE_GROUP" << endl;
                    DeleteNodeGroupTest(connection, stat);
                    break;
                }
                default: {
                    cout << "INVALID_TEST_TYPE" << endl;
                }
            }
            stat.PrintStat();
            cout << endl;
            allStat.push_back(stat);

            curIter++;
        }

        TestCaseStat::PrintAllStat(allStat);

        ostringstream oss;
        oss << strategy << "_" << value << "_result.csv";
        TestCaseStat::publishCsv(allStat, oss.str());
    }
}