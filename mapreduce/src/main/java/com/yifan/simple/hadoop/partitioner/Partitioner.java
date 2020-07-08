package com.yifan.simple.hadoop.partitioner;


public class Partitioner {

    private int numPartitions;

    public Partitioner() {
    }

    public Partitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getPatition(String key, int value, int numPatitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPatitions;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }
}
