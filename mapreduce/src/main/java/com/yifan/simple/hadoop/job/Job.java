package com.yifan.simple.hadoop.job;

import com.yifan.simple.hadoop.common.Context;
import com.yifan.simple.hadoop.inputformat.MapInputFormat;
import com.yifan.simple.hadoop.inputformat.ReducerInputFormat;
import com.yifan.simple.hadoop.mapper.Mapper;
import com.yifan.simple.hadoop.outputformat.MapOutputFormat;
import com.yifan.simple.hadoop.outputformat.ReducerOutputFormat;
import com.yifan.simple.hadoop.partitioner.Partitioner;
import com.yifan.simple.hadoop.reducer.Reducer;
import com.yifan.simple.hadoop.sort.Sort;
import com.yifan.simple.hadoop.split.Spliter;
import com.yifan.simple.hadoop.task.InputSplit;
import com.yifan.simple.hadoop.task.MapTaskRunner;
import com.yifan.simple.hadoop.task.ReduceTaskRunner;
import com.yifan.simple.hadoop.util.PathUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Job {

    private Context context;

    // 线程池：一个线程用来执行一个一个Task
    private ExecutorService executor = null;

    public Job(Context context) {
        this.context = context;
        // 初始化一个线程池
        int cpuCore = Runtime.getRuntime().availableProcessors();
        executor = Executors.newFixedThreadPool(cpuCore);
    }

    public void submit() {
        runJob(context);
    }

    private void runJob(Context context) {

        // 初始化job，主要是生成jobID，后续可以添加其他的功能
        initJob(context);

        // 逻辑切片
        Spliter spliter = new Spliter();
        // InputSplit 每个逻辑切片，包装了个数据块。 最终会启动一个任务的
        List<InputSplit> splits = spliter.getSplits(context, context.getInputDir());

        // 启动第一阶段计算
        runMapper(context, splits);

        // 启动shuffle阶段
        runShuffle(context);

        // 启动第二阶段计算
        runReducer(context);

        // 执行完成
        System.out.println("Job completed ... ");
    }

    private void runMapper(Context context, List<InputSplit> splits) {
        List<Future<Boolean>> futures = new ArrayList<>();

        // 提交任务执行，通过线程池的方式
        for (InputSplit split : splits) {
            // 封装一个TaskRunner
            MapTaskRunner taskRunner = new MapTaskRunner(context, split);
            Future<Boolean> future = executor.submit(taskRunner);
            futures.add(future);
        }
        System.out.println("Mapper 阶段Task任务提交完毕");

        // 检测是否完成
        checkTaskComplete(futures);
        System.out.println("Mapper阶段执行完毕");
    }

    private void runReducer(Context context) {

        List<Future<Boolean>> futures = new ArrayList<>();

        String jobID = context.getJobID();
        int partitions = context.getNumPartitions();

        /**
         * 有几个分区，就创建几个 ReduceTask
         */
        for (int i = 0; i < partitions; i++) {
            ReduceTaskRunner reduceTaskRunner = new ReduceTaskRunner(jobID, i, context);
            Future<Boolean> future = executor.submit(reduceTaskRunner);
            futures.add(future);
        }
        System.out.println("Reducer 阶段Task任务提交完毕");

        // 检测是否完成
        checkTaskComplete(futures);
        System.out.println("Reducer阶段执行完成");
    }

    private void runShuffle(Context context) {

        // 执行排序！ 我把逻辑放在了 MapOutputFormat中了的输出中了。
        // 分区也在输出数据的时候自行执行了。

        System.out.println("Shuffle阶段执行完成");
    }

    // 确认 task 的任务是否执行完成
    private void checkTaskComplete(List<Future<Boolean>> futures) {
        // 等待任务完成
        for (Future<Boolean> f : futures) {
            try {
                // 阻塞实现
                Boolean aBoolean = f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    public void setReducerOutputFormat(Class<ReducerOutputFormat> reducerOutputFormat) {
        context.setReducerOutputFormat(reducerOutputFormat);
    }

    public void setReducerInputFormat(Class<ReducerInputFormat> reducerInputFormat) {
        context.setReducerInputFormat(reducerInputFormat);
    }

    public void setReducer(Class<Reducer> reducer) {
        context.setReducer(reducer);
    }

    public void setSort(Class<Sort> sort) {
        context.setSort(sort);
    }

    public void setPartitioner(Class<Partitioner> partitioner) {
        context.setPartitioner(partitioner);
    }

    public void setMapOutputFormat(Class<MapOutputFormat> mapOutputFormat) {
        context.setMapOutputFormat(mapOutputFormat);
    }

    public void setMapper(Class<Mapper> mapper) {
        context.setMapper(mapper);
    }

    public void setMapInputFormat(Class<MapInputFormat> mapInputFormat) {
        context.setMapInputFormat(mapInputFormat);
    }

    public void setInputPath(String inputDir) {
        context.setInputDir(inputDir);
    }

    public void setOutputPath(String outputDir) {
        context.setOutputDir(outputDir);
    }

    /**
     * 初始化JobID
     *
     * @param context
     */
    private void initJob(Context context) {

        //检查输出目录
        String outputDir = context.getOutputDir();
        File file = new File(outputDir);
        if (file.listFiles().length != 0) {
            PathUtil.delete(outputDir);
            PathUtil.mkdir(outputDir);
        }
        String jobID = "nxmr_job_" + new Date().getTime();
        context.setJobID(jobID);
    }

    public void setNumPartitions(int numPartitions) {
        context.setNumPartitions(numPartitions);
    }

    public int getNumPartitions(int numPartitions) {
        return context.getNumPartitions();
    }
}
