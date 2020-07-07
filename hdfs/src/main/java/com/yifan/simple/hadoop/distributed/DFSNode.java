package com.yifan.simple.hadoop.distributed;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件系统中的一个节点
 **/
public class DFSNode {

    // 节点名称
    private String nodeName = null;
    private boolean isFile = false;

    // 父目录一定是目录
    private DFSNode parentNode = null;
    // 子目录列表就可能是：文件 或者 文件夹
    private List<DFSNode> childList = new ArrayList<>();

    // 构造方法
    public DFSNode(String nodeName, boolean isFile) {
        this.nodeName = nodeName;
        this.isFile = isFile;
    }

    /**
     * 获取某个节点下的所有子节点
     */
    public List<DFSNode> getChildren() {
        return childList;
    }

    /**
     * 给某个节点下增加子节点
     */
    public void addChildNode(String path) {
        File file = new File(path);
        if (file.isFile()) {
            childList.add(new DFSNode(path, true));
        } else {
            childList.add(new DFSNode(path, false));
        }
    }

    /**
     * 移除一个子节点
     */
    public void removeChildNode(String nodeName) {

    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public boolean isFile() {
        return isFile;
    }

    public void setFile(boolean file) {
        isFile = file;
    }
}
