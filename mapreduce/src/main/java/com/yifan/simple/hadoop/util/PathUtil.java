package com.yifan.simple.hadoop.util;

import java.io.File;
import java.text.DecimalFormat;

public class PathUtil {

    public static void main(String[] args) {

        System.out.println(generateMapOutputTempFileName(22));
    }

    public static String checkPath(String path) {
        return path.replace("\\", "/").replace("//", "/");
    }

    public static String generateMapOutputTempFileName(int partitionNum) {
        return "part_m_" + validatePartitionNum(partitionNum);
    }

    public static String generateReduceOutputTempFileName(int partitionNum) {
        return "part_r_" + validatePartitionNum(partitionNum);
    }

    private static String validatePartitionNum(int partitionNum) {
        DecimalFormat decimalFormat = new DecimalFormat("00000");
        return decimalFormat.format(partitionNum);
    }

    public static void mkdir(String mkdir) {

        // 当前目录
        File currentDir = new File(mkdir);

        // 父目录
        File parentDir = currentDir.getParentFile();

        // 如果不存在，递归创建
        if (!parentDir.exists()) {
            mkdir(currentDir.getParent());
        }
        currentDir.mkdir();
    }

    public static void delete(String dirPath) {
        File file = new File(dirPath);
        if (file.isFile()) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            if (files == null) {
                file.delete();
            } else {
                for (int i = 0; i < files.length; i++) {
                    delete(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }
}
