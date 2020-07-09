package com.yifan.simple.hadoop.outputformat;

import com.yifan.simple.hadoop.common.Context;
import com.yifan.simple.hadoop.partitioner.Partitioner;
import com.yifan.simple.hadoop.sort.Sort;
import com.yifan.simple.hadoop.task.Record;
import com.yifan.simple.hadoop.util.PathUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


public class MapOutputFormat {

    // 每个Task的输出
    private String outputPath;
    // 该Task属于哪个job
    private String jobID;
    // taskID
    private String taskID;

    private List<PrintWriter> writers;
    private Partitioner partitioner;
    private int numPartitions;
    private Sort sort;
    private List<ArrayList<Record>> recordsList;

    public MapOutputFormat() {
    }

    public MapOutputFormat(String tempOutputPath, Context context, String taskID) {

        /**
         * 一些必要初始化
         */
        initialize(context, taskID);

        String jobID = context.getJobID();

        // 新建临时目录，先判断是否存在。
        String tempTaskDir = PathUtil.checkPath(tempOutputPath + "/" + jobID + "/" + this.getTaskID());
        File file = new File(tempTaskDir);
        if (!file.exists()) {
            PathUtil.mkdir(tempTaskDir);
        }

        // 把临时目录保存起来， reducer阶段拉取数据要用。
        context.getMapTaskTempDirs().add(tempTaskDir);

        // 初始化输出流
        int partitions = context.getNumPartitions();
        writers = new ArrayList<>(partitions);

        for (int i = 0; i < partitions; i++) {
            // 完成的临时文件名称： tempOutputPath + jobID + part_m_00001
            String perfectTempFilePath = tempTaskDir + "/" + PathUtil
                    .generateMapOutputTempFileName(i) + ".txt";
            perfectTempFilePath = PathUtil.checkPath(perfectTempFilePath);

            PrintWriter printWriter = null;
            try {
                printWriter = new PrintWriter(new FileOutputStream(new File(perfectTempFilePath)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            writers.add(printWriter);
        }
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    private void initialize(Context context, String taskID) {
        this.taskID = taskID;
        this.numPartitions = context.getNumPartitions();
        try {
            this.partitioner = context.getPartitioner().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        this.recordsList = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            recordsList.add(new ArrayList<>());
        }
        try {
            this.sort = context.getSort().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写出数据的方法， 先收集到内存
     *
     * @param keyout
     * @param valueout
     */
    public void write(String keyout, int valueout) {

        int patition = partitioner.getPatition(keyout, valueout, numPartitions);

        // 此处这样设计，会比较消费内存。
        ArrayList<Record> records = recordsList.get(patition);
        records.add(new Record(keyout, valueout));
    }

    /**
     * 内存中的数据落盘,  落盘之前
     */
    public void spill(Sort sort) {
        // 再输出结果到磁盘
        for (int i = 0; i < recordsList.size(); i++) {
            PrintWriter printWriter = writers.get(i);
            ArrayList<Record> records = recordsList.get(i);
            // 排序
            sort.sort(records);
            for (int j = 0; j < records.size(); j++) {

                // 落盘
                printWriter.println(records.get(j));
            }
        }
    }

    /**
     * 关闭输出流
     */
    public void close() {
        spill(sort);
        for (PrintWriter out : writers) {
            out.close();
        }
    }
}
