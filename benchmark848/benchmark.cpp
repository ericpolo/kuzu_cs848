#include <iostream>
#include <string>
#include "build/debug/src/kuzu.hpp"
#include <algorithm>

using namespace kuzu::main;
using namespace std;

/* We create three node tables: People, Customer, and Organization. All three tables contain 10k entries */
const int NUM_TABLES = 3;
const int NUM_ROWS = 10000;
const string tableNames[NUM_TABLES] = {"People", "Customer", "Organization"};
const string tableCreateQuery [NUM_TABLES] = {
    "CREATE NODE TABLE People (id INT32, firstName STRING, lastName STRING, sex STRING, email STRING, phone STRING, jobTitle STRING, PRIMARY KEY(id));",
    "CREATE NODE TABLE Customer (id INT32, firstName STRING, lastName STRING, company STRING, city STRING, country STRING, primaryPhone STRING, secondaryPhone STRING, email STRING, website STRING, PRIMARY KEY(id));",
    "CREATE NODE TABLE Organization (id INT32, name STRING, website STRING, country STRING, description STRING, foundYear INT16, industry STRING, numEmployee INT16, PRIMARY KEY(id));"
};
/* will be updated later */
string tableCopyQuery [NUM_TABLES] = {"", "", ""};

const vector<string> tableColumns[NUM_TABLES] = {{"id", "firstName", "lastName", "sex", "email", "phone", "jobTitle"},
    {"id", "firstName", "lastName", "company", "city", "country", "primaryPhone", "secondaryPhone", "email", "website"},
    {"id", "name", "website", "country", "description", "foundYear", "industry", "numEmployee"}};
const int tableNumColumns[3] = {7, 10, 8};

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

/* Helper functions */
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

void Checkpoint(const unique_ptr<Connection> &connection)
{
    connection->query("CHECKPOINT;");
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
void DropTableTest(const unique_ptr<Connection> &connection)
{
    string tableName = RandomSelectTable("");
    string nextTableName = RandomSelectTable(tableName);

    /*
     * 1. Create table here first
     *      Checkpoint will create physical storage
     */
    CreateTable(connection, tableName);
    Checkpoint(connection);

    /*
     * 2. Drop the table and Create another table.
     *      Dropped chunks are recycled in DROP.
     *      Checkpoint will create physical storage for new table
     */
    DropTable(connection, tableName);
    CreateTable(connection, nextTableName);
    Checkpoint(connection);

    /* 3. Drop the second tables here to wrap up the test */
    DropTable(connection, nextTableName);
    Checkpoint(connection);
}

/*
 * Alter Table Test
 * This test is limited to test Alter Table DROP ... for now
 */
void AlterTableTest(const unique_ptr<Connection> &connection)
{
    string tableName = RandomSelectTable("");
    int tableIndex = FindTableIndex(tableName);
    string nextTableName = RandomSelectTable(tableName);

    /*
     * 1. Create table here first
     *      Checkpoint will create physical storage
     */
    CreateTable(connection, tableName);
    Checkpoint(connection);

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
    Checkpoint(connection);

    /* 3. Drop both tables here to wrap up the test */
    DropTable(connection, tableName);
    DropTable(connection, nextTableName);
    Checkpoint(connection);
}

void displayQueryResult(const unique_ptr<QueryResult> &result)
{
    while (result->hasNext()) {
        cout << "| ";
        const auto row = result->getNext();
        if (row->len() == 3) {
            cout << row->getValue(0)->getValue<string>() << " | "
                << row->getValue(1)->getValue<int64_t>() << " | "
                << row->getValue(2)->getValue<int64_t>() << " | " << endl;
        } else {
            cout << row->getValue(0)->getValue<string>() << " | ";
        }
    }
}

/* Delete Node Group Test */
void DeleteNodeGroupTest(const unique_ptr<Connection> &connection) {
    string tableName = RandomSelectTable("");
    string nextTableName = RandomSelectTable(tableName);

    /*
     * 1. Create table here first
     *      Checkpoint will create physical storage
     */
    CreateTable(connection, tableName);
    Checkpoint(connection);

    /*
     * 2. Delete table entries based on random ratio
     *      chunks are recycled in Checkpoint.
     */
    int beginId = (rand() % 50) * NUM_ROWS / 100;
    int endId = (rand() % 50 + 50) * NUM_ROWS / 100;
    DeleteEntries(connection, tableName, beginId, endId);
    Checkpoint(connection);

    /*
     * 3. Create a new table to resue recycled spaces
     */
    CreateTable(connection, nextTableName);
    Checkpoint(connection);

    /* 4. Drop both tables here to wrap up the test */
    DropTable(connection, tableName);
    DropTable(connection, nextTableName);
    Checkpoint(connection);
}

int main(int argc, char* argv[])
{
    if (argc == 1) {
        cout<<"Please type following input:"<<endl
            <<"    -N <number of iteration you want to run>"<<endl
            <<"    -D <directory that saves the csv source file>"<<endl;
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
        cout<<"Please use -N to specify the directory that saves the csv source file"<<endl;
        return 0;
    }
    maxIteration = std::atoi(iterationsArg);
    cout<<"User Parameters:"<<endl
        <<"    Csv file source directory: "<<csvFileDir<<endl
        <<"    Max test iterations: "<<maxIteration<<endl;

    /* Create an empty on-disk database and connect to it */
    kuzu::main::SystemConfig systemConfig;
    auto database = make_unique<Database>("testDB", systemConfig);
    auto connection = make_unique<Connection>(database.get());

    if (connection != nullptr) {
        int curIter = 0;
        while (curIter < maxIteration) {
            cout << "---------------------------\nBegin " + to_string(curIter) + "th iterations\n---------------------------" << endl;
            TestType testCase = (TestType)(rand() % 3);
            switch (testCase) {
                case DROP_TABLE: {
                    /* ERICTODO: Calculate average checkpoint time here */
                    cout << "Test Type: DROP_TABLE" << endl;
                    DropTableTest(connection);
                    break;
                }
                case ALTER_TABLE: {
                    /* ERICTODO: Calculate average checkpoint time here */
                    cout << "Test Type: ALTER_TABLE" << endl;
                    AlterTableTest(connection);
                    break;
                }
                case DELETE_NODE_GROUP: {
                    /* ERICTODO: Calculate average checkpoint time here */
                    cout << "Test Type: DELETE_NODE_GROUP" << endl;
                    DeleteNodeGroupTest(connection);
                    break;
                }
            }
            curIter++;
        }
    }
}