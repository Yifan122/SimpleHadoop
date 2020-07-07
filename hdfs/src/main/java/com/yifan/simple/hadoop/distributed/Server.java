package com.yifan.simple.hadoop.distributed;

/**
 * 一个服务器
 **/
public class Server {

    private String serverPath = null;

    public Server(String serverPath) {
        this.serverPath = serverPath;
    }

    public Server() {
    }

    public String getServerPath() {
        return serverPath;
    }

    public void setServerPath(String serverPath) {
        this.serverPath = serverPath;
    }
}
