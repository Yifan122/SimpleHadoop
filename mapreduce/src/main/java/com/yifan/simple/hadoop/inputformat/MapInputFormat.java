package com.yifan.simple.hadoop.inputformat;

import java.io.*;

public class MapInputFormat {

    private long key;
    private String value;

    // 存储当前这个filePath该存储的
    private String blockPath;

    // 逐行读取器
    private BufferedReader reader = null;

    public MapInputFormat(String inputPath) {
        this.blockPath = inputPath;
        this.reader = initReader(blockPath);
    }

    public MapInputFormat() {
    }

    public BufferedReader initReader(String filePath) {
        key = 0;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filePath))));
        } catch (FileNotFoundException e) {
            System.out.println("读取文件出错");
        }
        return br;
    }

    public boolean nextKeyValue() {
        try {
            value = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (value == null) {
            close();
            return false;
        } else {
            key += value.length();
            return true;
        }
    }

    public long getCurrentKey() {
        return key;
    }

    public String getCurrentValue() {
        return value;
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
