package com.yifan.simple.hadoop.task;

/**
 * 描述： 用于每个Task做计算的逻辑切片。用来存储每个块的信息
 */
public class InputSplit {

    private String blockId;         // blockID
    private String blockName;       // block名称
    private String blockPath;       // block的路径
    private String serverNode;      // 服务节点

    public InputSplit() {
    }

    public InputSplit(String blockPath) {
        this.blockPath = blockPath;
    }

    public InputSplit(String blockId, String blockPath) {
        this.blockId = blockId;
        this.blockPath = blockPath;
    }

    public InputSplit(String blockId, String blockName, String blockPath, String serverNode) {
        this.blockId = blockId;
        this.blockName = blockName;
        this.blockPath = blockPath;
        this.serverNode = serverNode;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getBlockName() {
        return blockName;
    }

    public void setBlockName(String blockName) {
        this.blockName = blockName;
    }

    public String getBlockPath() {
        return blockPath;
    }

    public void setBlockPath(String blockPath) {
        this.blockPath = blockPath;
    }

    public String getServerNode() {
        return serverNode;
    }

    public void setServerNode(String serverNode) {
        this.serverNode = serverNode;
    }

    @Override
    public String toString() {
        return blockId + "\t" + blockName + "\t" + blockPath + "\t" + serverNode;
    }
}
