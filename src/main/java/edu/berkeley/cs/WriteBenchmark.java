package edu.berkeley.cs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

class WriteBenchmark extends JDBCBenchmark {
    WriteBenchmark(String host, int batchSize, int numIter, int numThreads) {
        super(host, batchSize, numIter, numThreads);
        createTable();
    }

    public class WriterTask implements Callable<Result> {
        public Result call() throws Exception {
            Connection conn = createConnection();
            PreparedStatement statement = prepareWriteStatement(conn);
            long startTime = System.currentTimeMillis();
            writeDataToTable(conn, statement, getNumIter() * getBatchSize());
            try {
                statement.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }

            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            double thput = (double) (getNumIter() * getBatchSize()) / (totTime / 1000.0);
            double latency = (double) (totTime) / (getNumIter());

            return new Result(thput, latency);
        }
    }

    void runBenchmark() {
        LOG.info("Running Write Benchmark");
        ExecutorService executor = Executors.newFixedThreadPool(getNumThreads());
        List<WriterTask> tasks = new ArrayList<WriterTask>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new WriterTask());
        }
        List<Future<Result>> futures = null;
        try {
            futures = executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        double throughput = 0;
        double latency = 0;
        for (Future<Result> future : futures) {
            try {
                Result result = future.get();
                throughput += result.getThroughput();
                latency += result.getLatency();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

        }
        latency /= getNumThreads();
        System.out.println(throughput + " " + latency);
        executor.shutdown();
    }
}
