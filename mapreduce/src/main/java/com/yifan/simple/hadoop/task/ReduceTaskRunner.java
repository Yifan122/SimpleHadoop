package com.yifan.simple.hadoop.task;

import com.yifan.simple.hadoop.common.Context;

import java.util.concurrent.Callable;

public class ReduceTaskRunner implements Callable<Boolean> {

    private int reduceTaskID_index;
    private String jobID;
    private ReduceTask reduceTask;
    private Context context;

    /**
     * @param jobID
     * @param i
     */
    public ReduceTaskRunner(String jobID, int i, Context context) {
        this.reduceTaskID_index = i;
        this.jobID = jobID;
        this.reduceTask = new ReduceTask(jobID, i);
        this.context = context;
    }

    @Override
    public Boolean call() throws Exception {

        System.out.println("ReduceTask开始执行");
        reduceTask.runReduceTask(context, reduceTaskID_index);

        return true;
    }
}
