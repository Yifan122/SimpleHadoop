package com.yifan.simple.hadoop.sort;

import com.yifan.simple.hadoop.task.Record;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class Sort {

    public void sort(List<Record> records) {

        Collections.sort(records, new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {
                return (int) o1.getKey().compareTo(o2.getKey());
            }
        });
    }
}
