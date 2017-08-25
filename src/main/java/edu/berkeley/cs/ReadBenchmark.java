package edu.berkeley.cs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ReadBenchmark extends JDBCBenchmark {
    String readStmt;

    ReadBenchmark(String host, String dbName, String tableName, int batchSize, int numIter, int numThreads, String dataSource) {
        super(host, dbName, tableName, batchSize, numIter, numThreads, dataSource);
        this.readStmt = "SELECT * FROM " + tableName + " WHERE time >= ? AND time <= ?);";
        createTable();
        populateTable();
    }

    private PreparedStatement prepareStatement(Connection conn) {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(readStmt);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return statement;
    }

    private void prepareQuery(PreparedStatement statement) {
        int dataIdx = (new Random()).nextInt(numPoints() - getBatchSize());
        try {
            statement.setTimestamp(1, dataPoint(dataIdx).time);
            statement.setTimestamp(2, dataPoint(dataIdx + getBatchSize()).time);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    class ReaderTask implements Callable<Result> {

        public Result call() throws Exception {
            Connection conn = createConnection();
            PreparedStatement statement = prepareStatement(conn);
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < getNumIter(); i++) {
                prepareQuery(statement);
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
                assert(count == getBatchSize());
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
