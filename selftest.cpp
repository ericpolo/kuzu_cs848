#include <iostream>
#include "build/release/src/kuzu.hpp"

using namespace kuzu::main;
using namespace std;

void test1(const unique_ptr<Connection> &connection) {
    connection->query("CREATE NODE TABLE Person (name STRING, age INT64, height INT64, PRIMARY KEY(name));");
    connection->query("CREATE (a:Person {name: 'Adam1', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa1', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang1', age: 50, height:177});");
    connection->query("CREATE (a:Person {name: 'Adam2', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa2', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang2', age: 50, height:177});");
    connection->query("CREATE (a:Person {name: 'Adam3', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa3', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang3', age: 50, height:177});");
    connection->query("CHECKPOINT;");
    connection->query("DROP TABLE Person;");
    connection->query("CREATE NODE TABLE Person2 (name STRING, age INT64, height INT64, PRIMARY KEY(name));");
    connection->query("CREATE (a:Person2 {name: 'Adams', age: 30, height:183});");
    connection->query("CREATE (a:Person2 {name: 'Karissas', age: 40, height:156});");
    connection->query("CREATE (a:Person2 {name: 'Zhangs', age: 50, height:177});");
    connection->query("CHECKPOINT;");
    connection->query("DROP TABLE Person2;");
}

void test2(const unique_ptr<Connection> &connection) {
    connection->query("CREATE NODE TABLE Person (name STRING, age INT64, height INT64, PRIMARY KEY(name));");
    connection->query("CREATE (a:Person {name: 'Adam', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang', age: 50, height:177});");
    connection->query("CHECKPOINT;");
    connection->query("ALTER TABLE Person DROP height;");
    connection->query("ALTER TABLE Person DROP age;");
    connection->query("CREATE NODE TABLE Person2 (name STRING, age INT64, height INT64, PRIMARY KEY(name));");
    connection->query("CREATE (a:Person2 {name: 'Adam', age: 30, height:183});");
    connection->query("CREATE (a:Person2 {name: 'Karissa', age: 40, height:156});");
    connection->query("CREATE (a:Person2 {name: 'Zhang', age: 50, height:177});");
    connection->query("CHECKPOINT;");
}

void displayQueryResult(const unique_ptr<QueryResult> &result) {
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

void test3(const unique_ptr<Connection> &connection) {
    connection->query("CREATE NODE TABLE Person (name STRING, age INT64, height INT64, PRIMARY KEY(name));");
    connection->query("CREATE (a:Person {name: 'Adam1', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa1', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang1', age: 50, height:177});");
    connection->query("CHECKPOINT;");
    connection->query("CREATE (a:Person {name: 'Adam2', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa2', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang2', age: 50, height:177});");
    connection->query("CREATE (a:Person {name: 'Adam3', age: 30, height:183});");
    connection->query("CREATE (a:Person {name: 'Karissa3', age: 40, height:156});");
    connection->query("CREATE (a:Person {name: 'Zhang3', age: 50, height:177});");
    auto result = connection->query("MATCH (p:Person) DELETE p RETURN p.*;");
    displayQueryResult(result);
    connection->query("CHECKPOINT;");
    result = connection->query("MATCH (a:Person) RETURN a;");
    displayQueryResult(result);
}

int main()
{
    // Create an empty on-disk database and connect to it
    kuzu::main::SystemConfig systemConfig;
    auto database = make_unique<Database>("test", systemConfig);

    // Connect to the database.
    auto connection = make_unique<Connection>(database.get());

    cout << "Kuzu Connection: " << connection << endl;
    if (connection != nullptr) {
        cout << "\n---------------------------\nRunning Drop Table test\n---------------------------\n" << endl;
        test1(connection);
        cout << "\n---------------------------\nDrop Table test completed\n---------------------------\n" << endl;
        cout << "\n---------------------------\nRunning Drop Column test\n---------------------------\n" << endl;
        test2(connection);
        cout << "\n---------------------------\nDrop Column test completed\n---------------------------\n" << endl;
        cout << "\n---------------------------\nRunning Delete NodeGroup test\n---------------------------\n" << endl;
        test3(connection);
        cout << "\n---------------------------\nDelete NodeGroup test completed\n---------------------------\n" << endl;
    }
}
