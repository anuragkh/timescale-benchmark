package edu.berkeley.cs;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

abstract class JDBCBenchmark {
    static String TABLE_NAME = "test";

    private String hostName;
    private int batchSize;
    private int numIter;
    private int numThreads;
    private String dataSource;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
    }
    final static Logger LOG = Logger.getLogger(JDBCBenchmark.class.getName());

    class DataPoint {
        Timestamp time;
        double value;

        DataPoint(long time, double value) {
            this.time = new Timestamp(time / 1000000L);
            this.time.setNanos((int) (time % 1000000L));
            this.value = value;
        }

        @Override
        public String toString() {
            return time.toString() + "\t" + value;
        }
    }
    private DataPoint[] data;

    JDBCBenchmark(String hostName, int batchSize, int numIter, int numThreads, String dataSource) {
        this.hostName = hostName;
        this.batchSize = batchSize;
        this.numIter = numIter;
        this.numThreads = numThreads;
        this.dataSource = dataSource;
        this.data = new DataPoint[getBatchSize() * getNumIter()];
        LOG.info("Created new benchmark with: ");
        LOG.info("\thost: " + hostName);
        LOG.info("\ttable: " + TABLE_NAME);
        LOG.info("\tbatch-size: " + batchSize);
        LOG.info("\tnum-iterations: " + numIter);
        LOG.info("\tnum-threads: " + numThreads);
        LOG.info("\tdata-source: " + dataSource);
        readCSV(dataSource);
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

    DataPoint dataPoint(int i) {
        return data[i];
    }

    int numPoints () {
        return data.length;
    }

    PreparedStatement prepareWriteStatement(Connection conn) {
        StringBuilder insertStmtBuilder = new StringBuilder("INSERT INTO " + TABLE_NAME + "(time, value) VALUES ");
        for (int i = 0; i < getBatchSize(); i++) {
            insertStmtBuilder.append("(?, ?) ");
        }
        insertStmtBuilder.append(";");
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(insertStmtBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return statement;
    }

    void prepareWriteBatch(PreparedStatement statement, int dataIdx) {
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

    PreparedStatement prepareReadStatement(Connection conn) {
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("SELECT * FROM " + TABLE_NAME + " WHERE time >= ? AND time <= ?;");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return statement;
    }

    void prepareQuery(PreparedStatement statement) {
        int dataIdx = (new Random()).nextInt(numPoints() - getBatchSize());
        try {
            statement.setTimestamp(1, dataPoint(dataIdx).time);
            statement.setTimestamp(2, dataPoint(dataIdx + getBatchSize()).time);
            System.out.println("Query: " + statement.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void readCSV(String dataSource) {
        LOG.info("Reading data from " + dataSource + "...");
        BufferedReader br = null;
        String line;
        String cvsSplitBy = ",";
        int i = 0;
        try {
            br = new BufferedReader(new FileReader(dataSource));
            while ((line = br.readLine()) != null && i < data.length) {
                // use comma as separator
                String[] csv = line.split(cvsSplitBy);
                if (csv.length != 2) {
                    System.out.println("Could not parse CSV line " + i + ": " + line);
                    System.exit(1);
                }
                data[i] = new DataPoint(Long.parseLong(csv[0]), Double.parseDouble(csv[1]));
                System.out.println(data[i].toString());
                ++i;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("Finished reading data: " + data.length + " data points.");
    }

    void writeDataToTable(Connection conn, PreparedStatement statement) {
        int dataIdx = 0;
        for (int i = 0; i < getNumIter(); i++) {
            prepareWriteBatch(statement, dataIdx);
            try {
                statement.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            dataIdx += getBatchSize();
        }
    }

    void populateTable() {
        LOG.info("Populating table " + TABLE_NAME + "...");
        Connection conn = createConnection();
        PreparedStatement statement = prepareWriteStatement(conn);
        writeDataToTable(conn, statement);
        try {
            statement.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        LOG.info("Finished populating table");
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
                    "CREATE TABLE "  + TABLE_NAME + "(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL);";
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
