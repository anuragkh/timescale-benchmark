package edu.berkeley.cs;

class LoadData extends JDBCBenchmark {
    LoadData(String host, int batchSize, int numIter, int numThreads, String dataSource) {
        super(host, batchSize, numIter, numThreads, dataSource);
    }

    void runBenchmark() {
        createTable();
        populateTable();
    }
}
