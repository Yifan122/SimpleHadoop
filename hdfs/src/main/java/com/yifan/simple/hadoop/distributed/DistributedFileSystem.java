package com.yifan.simple.hadoop.distributed;

import com.yifan.simple.hadoop.util.IOUtils;

import java.util.List;
import java.util.Map;

/**
 * 分布式文件系统实现
 **/
public class DistributedFileSystem implements FileSystem {

    // root
    String rootPath = "";
    DFSNode root = new DFSNode(rootPath, true);

    // 维护一个HashMap(key是路径，value是DFSNode对象)
    Map<String, DFSNode> pathNodeMap = null;

    /**
     * 构造方法要做的一件非常重要的事情，就是加载元数据 解析服务器
     */
    public DistributedFileSystem() {
        // 加载元数据
        DFSEditLog logReader = new DFSEditLog();
        pathNodeMap = logReader.loadLogByStart();
    }

    /**
     * 上传文件
     *
     * @param inputFile c:/aa.txt
     * @param outputDir /aa/bb/
     */
    @Override
    public void put(String inputFile, String outputDir) {

        // 上传文件
        IOUtils.copyFileToDFS(inputFile, outputDir);

        // 记录日志

    }

    @Override
    public void get(String inputFile, String outputDir) {

    }

    @Override
    public void mkdir(String mkdir) {

    }

    @Override
    public List<String> list(String inputPath) {
        return null;
    }
}
