package edu.berkeley.cs;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

abstract class JDBCBenchmark {
    private String hostName;
    private String tableName;
    private int batchSize;
    private int numIter;
    private int numThreads;
    private String dataSource;

    final static Logger LOG = Logger.getLogger(JDBCBenchmark.class.getName());

    class DataPoint {
        Timestamp time;
        double value;

        DataPoint(long time, double value) {
            this.time = new Timestamp(time);
            this.value = value;
        }

    }
    private DataPoint[] data;

    JDBCBenchmark(String hostName, String tableName, int batchSize, int numIter, int numThreads, String dataSource) {
        this.hostName = hostName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.numIter = numIter;
        this.numThreads = numThreads;
        this.dataSource = dataSource;
        this.data = new DataPoint[getBatchSize() * getNumIter()];
        LOG.info("Created new benchmark with: ");
        LOG.info("\thost: " + hostName);
        LOG.info("\ttable: " + tableName);
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

    void readCSV(String dataSource) {
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

    void populateTable() {
        LOG.info("Populating table " + tableName + "...");
        try {
            Connection conn = createConnection();
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            copyManager.copyIn("COPY " + tableName + " FROM STDIN", new FileReader(dataSource));
        } catch (Exception e) {
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
            String sql1 = "CREATE TABLE IF NOT EXISTS "  + tableName
                    + "(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL);";
            String sql2 = "TRUNCATE " + tableName + ";";
            String sql3 = "SELECT create_hypertable('" + tableName + "', 'time');";
            stmt.execute(sql1);
            stmt.executeUpdate(sql2);
            stmt.executeUpdate(sql3);
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
