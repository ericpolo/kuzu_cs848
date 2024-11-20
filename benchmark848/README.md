To run this benchmark, you need to have the following environment variables defined:

```
# kuzu env
export KUZU_REPO=<your Kuzu repo directory>
export LIBRARY_PATH="${KUZU_REPO}/build/debug/src:${KUZU_REPO}/build/release/src:$LIBRARY_PATH"
export LD_LIBRARY_PATH="${KUZU_REPO}/build/debug/src:${KUZU_REPO}/build/release/src:$LD_LIBRARY_PATH"
export PATH="~${KUZU_REPO}/build/debug/tools/shell:$PATH"
```

Then, after compiled Kuzu, use this command to compile the test:
1. If your Kuzu is compiled in debug mode:
    ```
    g++ benchmark.cpp -DDEBUG -std=c++2a -g -lkuzu -lpthread -I ${KUZU_REPO} -o 848benchmark
    ```
2. If your Kuzu is compiled in release mode:
    ```
    g++ benchmark.cpp -std=c++2a -lkuzu -lpthread -I ${KUZU_REPO} -o 848benchmark
    ```
Compilation will generate an executable named 848benchmark.

You could use -h option to see the list of parameters required by the test:
```
./848benchmark -h
```

848benchmark takes three parameters:
1. -N: number of iterations
2. -D: directory of csv data file
3. -B: directory of test database

To run the test, use this command:
```
rm -rf <test DB directory>
./848benchmark -N <test iterations> -D <csv source directory> -B <test DB directory>
```

Here is an example to run for 10 iterations:
```
./848benchmark -N 10 -D . -B ./testDB/
```
Remember to clean your test database directory before each run of the test to make sure collected test is accurate.
Or you could run them consecutively to get result for more iterations.

