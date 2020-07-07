package com.yifan.simple.hadoop.distributed;

import com.yifan.simple.hadoop.util.PropertiesUtil;

import java.util.ArrayList;
import java.util.List;

/**
 *
 **/
public class ServerLoader {

    public static List<Server> loadServer() {
        List<Server> servers = null;
        // 解析服务器
        servers = new ArrayList<Server>();
        String serverStr = PropertiesUtil.getProperty("servers");
        String[] ss = serverStr.split(",");
        for (String server : ss) {
            servers.add(new Server(server));
        }
        return servers;
    }
}
