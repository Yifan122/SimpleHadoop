package com.yifan.simple.hadoop.task;

import com.yifan.simple.hadoop.common.Context;

import java.util.concurrent.Callable;

public class MapTaskRunner implements Callable<Boolean> {

    private InputSplit split;
    private Context context;
    private MapTask mapTask;

    public MapTaskRunner(Context context, InputSplit split) {
        this.split = split;
        this.context = context;
        this.mapTask = new MapTask();
    }

    /**
     * 每个 MapTask 都要创建一个自己的 InputFormat 和 RecordReader
     */
    @Override
    public Boolean call() throws Exception {

        System.out.println("MapTask开始执行");
        mapTask.runMapTask(split, context);

        return true;
    }
}