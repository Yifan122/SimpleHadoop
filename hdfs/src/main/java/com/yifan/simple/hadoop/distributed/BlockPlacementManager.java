package com.yifan.simple.hadoop.distributed;

import com.yifan.simple.hadoop.util.PropertiesUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 主要负责给某一个数据块的多个副本选择多个服务器节点
 **/
public class BlockPlacementManager {

    /**
     * 用来在素有服务器中进行服务器的选择
     *
     * @param servers 现在所有服务器的列表
     * @return
     */
    public List<Server> choseServer(List<Server> servers) {

        // 最终返回的结果
        List<Server> chosedServer = new ArrayList<>();

        // 获取副本个数
        Random random = new Random();
        String block_replication = PropertiesUtil.getProperty("block_replication");
        int blocks = Integer.parseInt(block_replication);

        // 如果副本个数 > 服务器节点个数， 直接返回所有的服务器，每个服务器存储一份
        if (servers.size() <= blocks) {
            return servers;
        }

        for (int i = 0; i < blocks; i++) {

            // 我使用的策略：随机挑选策略！
            chosedServer.add(servers.remove(random.nextInt(servers.size())));
        }

        return chosedServer;
    }
}
