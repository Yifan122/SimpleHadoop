package com.yifan.simple.hadoop.task;

public class Record {

    private String key;
    private int value;

    public Record() {
    }

    public Record(String key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key + "\t" + value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
