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

848benchmark takes five parameters:
1. -N: number of iterations
2. -D: directory of csv data file
3. -B: directory of test database
4. -S: Test strategy - one of auto, fixed, round (for round-robin), and seed.
   * Auto strategy runs all tests in a random order
   * Fixed strategy runs one fixed test specified by -V for N iterations
   * Round strategy (round-robin) cycles through all tests starting from the one specified by -V
   * Seed strategy runs all tests randomly based on the custom seed specified by -V
5. -V: Value for the strategy
   * If strategy is auto, value is an optional argument and ignored
   * If strategy is seed, any integer is acceptable
   * For other strategy, the values should be as follows:
      1. 0 for DROP_TABLE
      2. 1 for DELETE_NODE_GROUP
      3. 2 for ALTER_TABLE

To run the test, use this command:
```
rm -rf <test DB directory>
./848benchmark -N <test iterations> -D <csv source directory> -B <test DB directory> -S <strategy> -V <value>
```

Here is an example to run for 10 iterations with automatic strategy:
```
./848benchmark -N 10 -D . -B ./testDB/ -S auto
```

Here is an example to run for 10 iterations with round-robin strategy that starts from DELETE_NODE_GROUP test:
```
./848benchmark -N 10 -D . -B ./testDB/ -S round -V 1
```
Note:
1. If you want to run benchmark without FCM enabled, please change the MACRO **ENABLE_FREE_CHUNK_MAP** in file src/include/storage/store/free_chunk_map.h to be false.
2. REMEMBER to clean your old build before rebuild kuzu when **ENABLE_FREE_CHUNK_MAP** is updated. Changing MACRO will not update package during the next rebuild which essentially does nothing; therefore, you may not see the performance difference in benchmark result.
3. Remember to clean your test database directory before each run of the test to make sure collected test is accurate Or you could run them consecutively to accumulate results for more iterations.

