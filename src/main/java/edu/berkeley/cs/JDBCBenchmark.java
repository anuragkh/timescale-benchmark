package edu.berkeley.cs;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

abstract class JDBCBenchmark {
    private static String TABLE_NAME = "pkt";
    private static int NUM_DATA_PTS = 100000000;
    private static byte[] DATA = new byte[54];

    private String hostName;
    private int batchSize;
    private int numIter;
    private int numThreads;

    private int resolution;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
    }
    final static Logger LOG = Logger.getLogger(JDBCBenchmark.class.getName());

    JDBCBenchmark(String hostName, int batchSize, int numIter, int numThreads) {
        this.hostName = hostName;
        this.batchSize = batchSize;
        this.numIter = numIter;
        this.numThreads = numThreads;
        LOG.info("Created new benchmark with: ");
        LOG.info("\thost: " + hostName);
        LOG.info("\ttable: " + TABLE_NAME);
        LOG.info("\tbatch-size: " + batchSize);
        LOG.info("\tnum-iterations: " + numIter);
        LOG.info("\tnum-threads: " + numThreads);
    }

    int getBatchSize() {
        return batchSize;
    }

    int getNumIter() {
        return numIter;
    }

    int getNumThreads() {
        return numThreads;
    }

    private Timestamp buildTimestamp(long ns) {
        Timestamp time = new Timestamp(ns / 1000000L);
        time.setNanos((int) (ns % 1000000L));
        return time;
    }

    PreparedStatement prepareWriteStatement(Connection conn) {
        StringBuilder insertStmtBuilder = new StringBuilder("INSERT INTO " + TABLE_NAME + "(time, value) VALUES ");
        for (int i = 0; i < getBatchSize() - 1; i++) {
            insertStmtBuilder.append("(?, ?), ");
        }
        insertStmtBuilder.append("(?, ?);");
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(insertStmtBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return statement;
    }

    private void prepareWriteBatch(PreparedStatement statement) {
        for (int i = 0; i < getBatchSize(); i++) {
            try {
                statement.setTimestamp(i * 2 + 1, buildTimestamp(System.nanoTime()));
                statement.setBytes(i * 2 + 2, DATA);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    void writeDataToTable(Connection conn, PreparedStatement statement, int numDataPts) {
        int iMax = numDataPts / batchSize;
        for (int i = 0; i < iMax; i++) {
            prepareWriteBatch(statement);
            try {
                statement.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    Connection createConnection() {
        String url = "jdbc:postgresql://" + hostName + "/postgres";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        Connection conn =  null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, props);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return conn;
    }

    void createTable() {
        try {
            Connection conn = createConnection();
            Statement stmt = conn.createStatement();
            String sql1 = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" +
                    "DROP TABLE IF EXISTS " + TABLE_NAME + " CASCADE;" +
                    "CREATE TABLE "  + TABLE_NAME + "(time TIMESTAMPTZ NOT NULL, value BYTEA NOT NULL);";
            String sql2 = "TRUNCATE " + TABLE_NAME + ";";
            String sql3 = "SELECT create_hypertable('" + TABLE_NAME + "', 'time');";
            stmt.execute(sql1);
            stmt.executeUpdate(sql2);
            stmt.execute(sql3);
            stmt.close();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    abstract void runBenchmark();
}
