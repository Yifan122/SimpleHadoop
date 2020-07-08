package com.yifan.simple.hadoop.driver;

import com.yifan.simple.hadoop.common.Context;
import com.yifan.simple.hadoop.inputformat.MapInputFormat;
import com.yifan.simple.hadoop.inputformat.ReducerInputFormat;
import com.yifan.simple.hadoop.job.Job;
import com.yifan.simple.hadoop.mapper.Mapper;
import com.yifan.simple.hadoop.outputformat.MapOutputFormat;
import com.yifan.simple.hadoop.outputformat.ReducerOutputFormat;
import com.yifan.simple.hadoop.partitioner.Partitioner;
import com.yifan.simple.hadoop.reducer.Reducer;
import com.yifan.simple.hadoop.sort.Sort;

public class Driver {

    public static void main(String[] args) {

        Context context = new Context();

        Job job = new Job(context);

        // MapInputFormat 负责给mapper阶段读取数据
        job.setMapInputFormat(MapInputFormat.class);
        // 负责第一阶段的逻辑处理的
        job.setMapper(Mapper.class);
        // MapOutputFormat 负责给第一个阶段输出结果的
        job.setMapOutputFormat(MapOutputFormat.class);

        // Partitioner 制定分区规则
        job.setPartitioner(Partitioner.class);
        job.setNumPartitions(3);
        // Sort 制定排序规则
        job.setSort(Sort.class);

        // 负责给 Reducer 读取数据的
        job.setReducerInputFormat(ReducerInputFormat.class);
        // Reducer 第二个阶段业务逻辑处理
        // 负责给reducer写出数据的
        job.setReducer(Reducer.class);
        job.setReducerOutputFormat(ReducerOutputFormat.class);

        // 指定真个应用程序的输入和输出
        job.setInputPath("C:\\Users\\Yifan\\SimpleHadoop\\abc.txt");
        job.setOutputPath("C:\\Users\\Yifan\\SimpleHadoop\\out");

        // 提交任务
        job.submit();

        System.exit(0);
    }
}
