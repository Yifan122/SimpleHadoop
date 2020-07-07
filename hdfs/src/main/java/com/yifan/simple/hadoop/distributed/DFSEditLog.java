package com.yifan.simple.hadoop.distributed;

import com.yifan.simple.hadoop.util.PropertiesUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 日志操作类
 **/
public class DFSEditLog {

    private PrintWriter out = null;

    public DFSEditLog() {
        try {
            out = new PrintWriter(new File(PropertiesUtil.getProperty("namespaceLogFile")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 记录元数据
     * 无论是上传或者下载，都是需要记录一条日志
     */
    public void appendLog(Log log) {
        out.println(log.toString());
    }

    /**
     * 读取日志文件，加载元数据
     */
    public Map<String, DFSNode> loadLogByStart() {

        Map<String, DFSNode> pathNodeMap = new HashMap<>();

        File logFile = new File(PropertiesUtil.getProperty("namespaceLogFile"));
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(logFile));
            // 加载数据
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                String nodeName = fields[0];
                String fsPath = fields[1];
                String putOrDelete = fields[2];

                // 解析元数据放入内存中。
                File file = new File(fsPath);
                if (Boolean.parseBoolean(putOrDelete)) {
                    pathNodeMap.put(fsPath, new DFSNode(nodeName, file.isFile() ? true : false));
                } else {
                    pathNodeMap.remove(fsPath);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("元数据日志丢失");
        } catch (IOException e) {
            System.out.println("读取文件内容异常");
        }

        return pathNodeMap;
    }
}
