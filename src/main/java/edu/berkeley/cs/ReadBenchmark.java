package edu.berkeley.cs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public class ReadBenchmark extends JDBCBenchmark {
    String readStmt;

    ReadBenchmark(String host, String dbName, String tableName, int batchSize, int numIter, String dataSource) {
        super(host, dbName, tableName, batchSize, numIter, dataSource);
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

    void runBenchmark() {
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
        System.out.println(thput + " " + latency);
    }
}
