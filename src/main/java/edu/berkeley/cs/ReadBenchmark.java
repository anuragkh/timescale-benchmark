package edu.berkeley.cs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class ReadBenchmark extends JDBCBenchmark {
    ReadBenchmark(String host, int batchSize, int numIter, int numThreads, String dataSource) {
        super(host, batchSize, numIter, numThreads, dataSource);
    }

    class ReaderTask implements Callable<Result> {
        public Result call() throws Exception {
            Connection conn = createConnection();
            PreparedStatement statement = prepareReadStatement(conn);
            double sum = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                prepareReadBatch(statement);
                int count = 0;
                try {
                    ResultSet rs = statement.executeQuery();
                    while(rs.next()) {
                        count++;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                sum += count;
                assert(count == getBatchSize());
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            if (sum != getNumIter() * getBatchSize()) {
                LOG.warning(String.format("sum(%f) != num-iterations(%d) * batch-size(%d)",
                        sum, getNumIter(), getBatchSize()));
            }
            double thput = sum / (totTime / 1000.0);
            double latency = (double) (totTime) / (getNumIter());
            try {
                statement.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            return new Result(thput, latency);
        }
    }

    void runBenchmark() {
        ExecutorService executor = Executors.newFixedThreadPool(getNumThreads());
        List<ReaderTask> tasks = new ArrayList<ReaderTask>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new ReaderTask());
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
