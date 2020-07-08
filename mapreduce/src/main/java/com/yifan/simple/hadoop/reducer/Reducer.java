package com.yifan.simple.hadoop.reducer;

import com.yifan.simple.hadoop.outputformat.ReducerOutputFormat;

import java.util.List;

/**
 * Author： 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 **/
public class Reducer {

    public void setup(ReducerOutputFormat reducerOutputFormat) {
        // 待用
    }

    public void run(ReducerOutputFormat reducerOutputFormat, String reduceTempFile) {
        setup(reducerOutputFormat);
        while (reducerOutputFormat.nextKeyValues()) {
            String key = reducerOutputFormat.getCurrentKey();
            List<Integer> values = reducerOutputFormat.getCurrentValues();

            reduce(key, values, reducerOutputFormat);
        }
        cleanup(reducerOutputFormat);
    }

    public void cleanup(ReducerOutputFormat reducerOutputFormat) {
        reducerOutputFormat.close();
    }

    public void reduce(String key, List<Integer> values, ReducerOutputFormat reducerOutputFormat) {
        int total = 0;
        for (int a : values) {
            total += a;
        }
        reducerOutputFormat.write(key, total);
    }
}
