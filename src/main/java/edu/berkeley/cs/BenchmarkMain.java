package edu.berkeley.cs;

import org.apache.commons.cli.*;

public class BenchmarkMain {
    public static void main(String[] args) {
        Options options = new Options();

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

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("bench", options);
            System.exit(1);
        }

        String host = cmd.getOptionValue("host", "localhost");
        int batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", "1"));
        int numIter = Integer.parseInt(cmd.getOptionValue("num-iterations", "1000"));
        int numThreads = Integer.parseInt(cmd.getOptionValue("num-threads", "1"));

        JDBCBenchmark bench = new WriteBenchmark(host, batchSize, numIter, numThreads);
        bench.runBenchmark();
    }
}