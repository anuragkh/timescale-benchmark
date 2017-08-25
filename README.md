# Benchmark for TimescaleDB
## Building 

This project uses maven to build:

```bash
mvn clean package
```

## Running

```
Usage: ./bin/bench
-b,--batch-size <arg>       Read/write batch size
-d,--data-source <arg>      Data source
-h,--host <arg>             Hostname
-n,--num-iterations <arg>   Number of iterations in benchmark
-t,--num-threads <arg>      Number of benchmark threads
```


