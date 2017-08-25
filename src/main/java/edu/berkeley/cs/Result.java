package edu.berkeley.cs;

public class Result {
    private double throughput;
    private double latency;


    public Result(double throughput, double latency) {
        this.throughput = throughput;
        this.latency = latency;
    }

    public double getThroughput() {
        return throughput;
    }

    public double getLatency() {
        return latency;
    }
}
