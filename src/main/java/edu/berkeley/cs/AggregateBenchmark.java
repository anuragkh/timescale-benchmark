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

class AggregateBenchmark extends JDBCBenchmark {
    AggregateBenchmark(String host, int batchSize, int numIter, int numThreads, String dataSource) {
        super(host, batchSize, numIter, numThreads, dataSource);
    }

    class AggregateTask implements Callable<Result> {
        public Result call() throws Exception {
            Connection conn = createConnection();
            PreparedStatement statement = prepareAggregateStatement(conn);
            double sum = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                prepareAggregateQuery(statement);
                int count = 0;
                try {
                    ResultSet rs = statement.executeQuery();
                    while(rs.next()) {
                        System.out.println(rs.getString(1) + ", " + rs.getString(2)
                                + ", " + rs.getString(3) + ", " + rs.getString(4));
                        count++;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                sum += count;
                assert(count == 1);
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
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
        List<AggregateTask> tasks = new ArrayList<AggregateTask>();
        for (int i = 0; i < getNumThreads(); i++) {
            tasks.add(new AggregateTask());
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
