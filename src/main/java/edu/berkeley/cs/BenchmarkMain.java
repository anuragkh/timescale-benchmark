package edu.berkeley.cs;

import org.apache.commons.cli.*;

public class BenchmarkMain {
    public static void main(String[] args) {
        Options options = new Options();

        Option benchTypeOpt = new Option("T", "type", true, "Benchmark type (read/write)");
        benchTypeOpt.setType(String.class);
        options.addOption(benchTypeOpt);

        Option hostOpt = new Option("h", "host", true, "Hostname");
        hostOpt.setType(String.class);
        options.addOption(hostOpt);

        Option batchSizeOpt = new Option("b", "batch-size", true, "Read/write batch size");
        batchSizeOpt.setType(Integer.class);
        options.addOption(batchSizeOpt);

        Option numBatchesOpt = new Option("n", "num-iterations", true, "Number of iterations in benchmark");
        numBatchesOpt.setType(Integer.class);
        options.addOption(numBatchesOpt);

        Option numThreadsOpt = new Option("t", "num-threads", true, "Number of benchmark threads");
        numThreadsOpt.setType(Integer.class);
        options.addOption(numThreadsOpt);

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
        String dbName = "test";
        String tableName = "test";
        int batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", "1"));
        int numIter = Integer.parseInt(cmd.getOptionValue("num-iterations", "1000"));
        int numThreads = Integer.parseInt(cmd.getOptionValue("num-threads", "1"));
        String dataSource = cmd.getOptionValue("data-source");

        JDBCBenchmark bench = null;
        if (benchType.equalsIgnoreCase("write")) {
            bench = new WriteBenchmark(host, dbName, tableName, batchSize, numIter, numThreads, dataSource);
        } else if (benchType.equalsIgnoreCase("read")) {
            bench = new ReadBenchmark(host, dbName, tableName, batchSize, numIter, numThreads, dataSource);
        }
        assert bench != null;
        bench.runBenchmark();
    }
}