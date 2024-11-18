#include <iostream>
#include <string>
#include <chrono>
#include <algorithm>
#include <sys/stat.h>

#include "build/debug/src/kuzu.hpp"

using namespace kuzu::main;
using namespace std;
using namespace std::chrono;

/* We create three node tables: People, Customer, and Organization. All three tables contain 10k entries */
const int NUM_TABLES = 3;
const int NUM_ROWS = 10000;
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

enum AlterType : int
{
    DROP_COLUMN = 0,
    ADD_COLUMN = 1
};

enum AlterAddColumnType : int
{
    INVALID_COLUMN_TYPE = -1,
    INT64 = 0,
    BOOL = 1,
    STRING = 2
};

enum TestType : int
{
    DROP_TABLE = 0,
    ALTER_TABLE = 1,
    DELETE_NODE_GROUP = 2
};

/* This struct keep tracks of stat of each test case */
typedef struct TestCaseStat {
    /* Accumulated Checkpoint time */
    microseconds checkPointTimeAcc;
    /* Number of Checkpoint executed */
    int numCheckPoint;
    /* Duration of current test case */
    microseconds runningDuration;
    /* Data file size */
    long dataFileSize;

    void PrintStat() {
        cout << "Current test case stat:" << endl
             << "    checkPointTimeAccumulate: " << checkPointTimeAcc.count() <<" μs"<< endl
             << "    checkPointCounts:         " << numCheckPoint << endl
             << "    checkPointTimeAverage:    " << checkPointTimeAcc.count() / numCheckPoint << " μs" << endl
             << "    runningDuration:          " << runningDuration.count() << " μs" << endl
             << "    dataFileSize:             " << dataFileSize << " bytes" << endl;
    }

    static void PrintAllStat(vector<TestCaseStat> allStat) {
        TestCaseStat accStat = {microseconds(0), 0, microseconds(0), 0};

        for (auto stat : allStat) {
            accStat.checkPointTimeAcc += stat.checkPointTimeAcc;
            accStat.numCheckPoint += stat.numCheckPoint;
            accStat.runningDuration += stat.runningDuration;
            accStat.dataFileSize += stat.dataFileSize;
        }
        cout << "Overall test cases stat:" << endl
             << "    checkPointTimeAccumulate: " << accStat.checkPointTimeAcc.count() <<" μs"<< endl
             << "    checkPointCounts:         " << accStat.numCheckPoint << endl
             << "    checkPointTimeAverage:    " << accStat.checkPointTimeAcc.count() / accStat.numCheckPoint << " μs" << endl
             << "    runningDuration:          " << accStat.runningDuration.count() << " μs" << endl
             << "    dataFileSizeAvg:          " << accStat.dataFileSize / allStat.size() << " bytes" << endl
             << "    dataFileSizeFinal:        " << allStat.back().dataFileSize << " bytes" << endl;
    }
} TestCaseStat;

/* Miscellaneous Helper functions */
std::string GenRandomStr(const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    
    return tmp_s;
}

char* GetCmdOption(char ** begin, char ** end, const std::string & option)
{
    char ** itr = std::find(begin, end, option);
    if (itr != end && ++itr != end)
    {
        return *itr;
    }
    return nullptr;
}

void UpdateTableCopyQuery(string csvFileDir)
{
    tableCopyQuery[0] = "COPY People FROM '" + csvFileDir + +"/people-10000.csv';";
    tableCopyQuery[1] = "COPY Customer FROM '" + csvFileDir + +"/customers-10000.csv';";
    tableCopyQuery[2] = "COPY Organization FROM '" + csvFileDir + +"/organizations-10000.csv';";
}

long GetFileSize(string filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

string RandomSelectTable(string avoidTable)
{
    string tableName = avoidTable;
    while (tableName == avoidTable) {
        int tableIndex = rand() % NUM_TABLES;
        tableName = tableNames[tableIndex];
    }

    return tableName;
}

int FindTableIndex(string tableName)
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
    for (auto i = 0; i < NUM_TABLES; i++) {
        auto name = tableNames[i];
        if (tableName == name) {
            connection->query(tableCreateQuery[i]);
            connection->query(tableCopyQuery[i]);
        }
    }
}

void DropTable(const unique_ptr<Connection> &connection, string tableName)
{
    string query = "DROP TABLE " + tableName + ";";
    connection->query(query);
}

/* Note that colType is only valid when alterType is ADD_COLUMN. Otherwise, it should be set as INVALID_COLUMN_TYPE */
void AlterTable(const unique_ptr<Connection> &connection, string tableName, AlterType alterType, string colName, AlterAddColumnType colType)
{
    KU_ASSERT(alterType != ADD_COLUMN || colType == INVALID_COLUMN_TYPE);

    /* define base query here */
    string query = "ALTER TABLE" + tableName;
    switch (alterType) {
        case DROP_COLUMN:
            query += (" DROP COLUMN" + colName);
            break;
        case ADD_COLUMN:
            query += (" ADD COLUMN" + colName);
            switch (colType) {
                case INT64: {
                    int defaultVal = rand();
                    query += (" DEFAULT " + to_string(defaultVal));
                    break;
                }
                case BOOL: {
                    int defaultVal = rand() % 2;
                    string defaultStr = (defaultVal == 1)? "True":"False";
                    query += (" DEFAULT " + defaultStr);
                    break;
                }
                case STRING: {
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

    /* 3. Drop the second tables here to wrap up the test */
    DropTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 4. Update TestCaseStat here */
    stat.checkPointTimeAcc = ckptAccTime;
    stat.numCheckPoint = 3;
    stat.runningDuration = duration_cast<microseconds>(high_resolution_clock::now() - start);
    stat.dataFileSize = GetFileSize(dataFilePath);
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
    AlterTable(connection, tableName, DROP_COLUMN, dropColName, INVALID_COLUMN_TYPE);
    CreateTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 3. Drop both tables here to wrap up the test */
    DropTable(connection, tableName);
    DropTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 4. Update TestCaseStat here */
    stat.checkPointTimeAcc = ckptAccTime;
    stat.numCheckPoint = 3;
    stat.runningDuration = duration_cast<microseconds>(high_resolution_clock::now() - start);
    stat.dataFileSize = GetFileSize(dataFilePath);
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

    /* 4. Drop both tables here to wrap up the test */
    DropTable(connection, tableName);
    DropTable(connection, nextTableName);
    ckptAccTime += Checkpoint(connection);

    /* 5. Update TestCaseStat here */
    stat.checkPointTimeAcc = ckptAccTime;
    stat.numCheckPoint = 4;
    stat.runningDuration = duration_cast<microseconds>(high_resolution_clock::now() - start);
    stat.dataFileSize = GetFileSize(dataFilePath);
}


/* Main function for the benchmark */
int main(int argc, char* argv[])
{
    if (argc == 1) {
        cout<<"Please type following input:"<<endl
            <<"    -N <number of iteration you want to run>"<<endl
            <<"    -D <directory of the csv source files>"<<endl
            <<"    -B <directory of database>";
        return 0;
    }

    /* parse the argument first */
    char *csvFileDir = GetCmdOption(argv, argv + argc, "-D");
    if (csvFileDir == nullptr) {
        cout<<"Please use -D to specify the directory that saves the csv source file"<<endl;;
        return 0;
    }
    UpdateTableCopyQuery(csvFileDir);

    int maxIteration = 0;
    char *iterationsArg = GetCmdOption(argv, argv + argc, "-N");
    if (iterationsArg == 0) {
        cout<<"Please use -N to specify the directory that saves the csv source files"<<endl;
        return 0;
    }
    maxIteration = std::atoi(iterationsArg);

    char *databaseDir = GetCmdOption(argv, argv + argc, "-B");;
    if (databaseDir == nullptr) {
        cout<<"Please use -D to specify the directory that saves database files"<<endl;
        return 0;
    }
    databaseHomeDirectory = databaseDir;
    dataFilePath = databaseHomeDirectory + "/data.kz";

    cout<<"User Parameters:"<<endl
        <<"    Csv file source directory: "<<csvFileDir<<endl
        <<"    Max test iterations: "<<maxIteration<<endl
        <<"    Database home directory: "<<databaseDir<<endl;

    /* Create an empty on-disk database and connect to it */
    kuzu::main::SystemConfig systemConfig;
    auto database = make_unique<Database>(databaseDir, systemConfig);
    auto connection = make_unique<Connection>(database.get());

    if (connection != nullptr) {
        int curIter = 1;
        vector<TestCaseStat> allStat = {};
        while (curIter <= maxIteration) {
            cout << "----------------------------\nBegin " + to_string(curIter) + "th iterations\n----------------------------" << endl;
            TestType testCase = (TestType)(rand() % 3);
            TestCaseStat stat = {microseconds(0), 0, microseconds(0), 0};
            switch (testCase) {
                case DROP_TABLE: {
                    cout << "Test Type: DROP_TABLE" << endl;
                    DropTableTest(connection, stat);
                    break;
                }
                case ALTER_TABLE: {
                    cout << "Test Type: ALTER_TABLE" << endl;
                    AlterTableTest(connection, stat);
                    break;
                }
                case DELETE_NODE_GROUP: {
                    cout << "Test Type: DELETE_NODE_GROUP" << endl;
                    DeleteNodeGroupTest(connection, stat);
                    break;
                }
            }
            stat.PrintStat();
            cout << endl;
            allStat.push_back(stat);

            curIter++;
        }

        TestCaseStat::PrintAllStat(allStat);
    }
}