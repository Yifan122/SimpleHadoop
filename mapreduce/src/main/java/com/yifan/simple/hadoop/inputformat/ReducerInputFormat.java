package com.yifan.simple.hadoop.inputformat;

import com.yifan.simple.hadoop.common.Context;
import com.yifan.simple.hadoop.sort.Sort;
import com.yifan.simple.hadoop.task.Record;
import com.yifan.simple.hadoop.util.PathUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ReducerInputFormat {

    // 用来做合并输出的流
    private PrintWriter printWriter;

    // 内存结构，存储数据
    private List<Record> records;

    public ReducerInputFormat(String inputPath) {

    }

    public ReducerInputFormat() {

    }

    /**
     * 分区数据合并  只拿自己的。
     *
     * @param context
     */
    public String mergeMapTempFiles(Context context, int reduceTaskID_index) {

        records = new ArrayList<>();

        int numPartitions = context.getNumPartitions();
        String jobID = context.getJobID();

        String reduceTaskTempDir = context
                .getConf("nxmapreduce_temp") + "/" + jobID + "/" + "nxmr_reduce_task_ptn" + reduceTaskID_index;
        reduceTaskTempDir = PathUtil.checkPath(reduceTaskTempDir);
        File reduceTempDir = new File(reduceTaskTempDir);
        if (!reduceTempDir.exists()) {
            PathUtil.mkdir(reduceTaskTempDir);
        }
        String reduceTaskTempFile = reduceTaskTempDir + "/" + "result_temp.txt";

        // 用来做shuffle之后reduce拉取数据做合并的数据流
        try {
            printWriter = new PrintWriter(new FileOutputStream(new File(reduceTaskTempFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // 处理输入和输出路径
        List<String> mapTaskTempDirs = context.getMapTaskTempDirs();

        // 把不同的mapTask的相同分区的结果数据拉取到一起，然后合并成一个有序的文件。
        for (int i = 0; i < mapTaskTempDirs.size(); i++) {
            File file = new File(mapTaskTempDirs.get(i));
            File[] tempFiles = file.listFiles();

            for (File f : tempFiles) {
                String ptn_index = f.getName().replace("part_m_", "").replace(".txt", "");
                if (Integer.parseInt(ptn_index) == reduceTaskID_index) {
                    readFileData(f);
                }
            }
        }

        // 数据刷写到磁盘
        spill(context);

        printWriter.close();

        return reduceTaskTempFile;
    }

    private void spill(Context context) {

        // 先排序
        Sort sort = null;
        try {
            sort = context.getSort().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        sort.sort(records);

        // 刷写
        for (Record r : records) {
            printWriter.println(r.getKey() + "\t" + r.getValue());
        }
    }

    /**
     * 读文件到磁盘
     *
     * @param f
     */
    private void readFileData(File f) {

        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(f));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] key_value = line.split("\t");
                records.add(new Record(key_value[0], Integer.parseInt(key_value[1])));
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
