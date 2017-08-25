package edu.berkeley.cs;

import org.apache.commons.cli.*;

public class BenchmarkMain {
    public static void main(String[] args) {
        Options options = new Options();

        Option benchTypeOpt = new Option("t", "type", true, "Benchmark type (read/write)");
        benchTypeOpt.setType(String.class);
        options.addOption(benchTypeOpt);

        Option hostOpt = new Option("h", "host", true, "Hostname");
        hostOpt.setType(String.class);
        options.addOption(hostOpt);

        Option dbNameOpt = new Option("s", "db", true, "Database name");
        dbNameOpt.setType(String.class);
        options.addOption(dbNameOpt);

        Option tableNameOpt = new Option("a", "table", true, "Table name");
        tableNameOpt.setType(String.class);
        options.addOption(tableNameOpt);

        Option batchSizeOpt = new Option("b", "batch-size", true, "Read/write batch size");
        batchSizeOpt.setType(Integer.class);
        options.addOption(batchSizeOpt);

        Option numBatchesOpt = new Option("n", "num-iterations", true, "Number of iterations in benchmark");
        numBatchesOpt.setType(Integer.class);
        options.addOption(numBatchesOpt);

        Option dataSourceOpt = new Option("d", "data-source", true, "Data source");
        dataSourceOpt.setRequired(true);
        dataSourceOpt.setType(String.class);
        options.addOption(dataSourceOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("timescale-bench", options);
            System.exit(1);
        }

        String benchType = cmd.getOptionValue("type", "write");
        String host = cmd.getOptionValue("host", "localhost");
        String dbName = cmd.getOptionValue("db", "test");
        String tableName = cmd.getOptionValue("table", "test");
        int batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", "1"));
        int numIter = Integer.parseInt(cmd.getOptionValue("num-iterations", "1000"));
        String dataSource = cmd.getOptionValue("data-source");

        JDBCBenchmark bench = null;
        if (benchType.equalsIgnoreCase("write")) {
            bench = new WriteBenchmark(host, dbName, tableName, batchSize, numIter, dataSource);
        } else if (benchType.equalsIgnoreCase("read")) {
            bench = new ReadBenchmark(host, dbName, tableName, batchSize, numIter, dataSource);
        }
        assert bench != null;
        bench.runBenchmark();
    }
}