package edu.berkeley.cs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class WriteBenchmark extends JDBCBenchmark {
    private String insertStmt;

    public WriteBenchmark(String host, String dbName, String tableName, int batchSize, int numIter, int numThreads, String dataSource) {
        super(host, dbName, tableName, batchSize, numIter, numThreads, dataSource);
        StringBuilder insertStmtBuilder = new StringBuilder("INSERT INTO " + tableName + "(time, value) VALUES ");
        for (int i = 0; i < getBatchSize(); i++) {
            insertStmtBuilder.append("(?, ?) ");
        }
        insertStmtBuilder.append(";");
        this.insertStmt = insertStmtBuilder.toString();
        createTable();
    }

    private PreparedStatement prepareStatement(Connection conn) {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(insertStmt);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return statement;
    }

    private void prepareBatch(PreparedStatement statement, int dataIdx) {
        for (int i = 0; i < getBatchSize(); i++) {
            try {
                DataPoint p = dataPoint(dataIdx + i);
                statement.setTimestamp(i * 2 + 1, p.time);
                statement.setDouble(i * 2 + 2, p.value);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public class WriterTask implements Callable<Result> {
        public Result call() throws Exception {
            Connection conn = createConnection();
            PreparedStatement statement = prepareStatement(conn);
            int dataIdx = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                prepareBatch(statement, dataIdx);
                try {
                    statement.executeUpdate();
                    conn.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                dataIdx += getBatchSize();
            }
            long endTime = System.currentTimeMillis();
            long totTime = endTime - startTime;
            double thput = (double) (getNumIter() * getBatchSize()) / (totTime / 1000.0);
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
