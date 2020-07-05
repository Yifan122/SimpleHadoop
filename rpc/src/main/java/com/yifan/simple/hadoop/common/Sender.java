package com.yifan.simple.hadoop.common;


import com.yifan.simple.hadoop.client.NettyRPCClient;
import com.yifan.simple.hadoop.protocol.RpcRequest;
import com.yifan.simple.hadoop.protocol.RpcResponse;

/**
 * 一个用来发送请求的工具类
 */
public class Sender {

    /**
     * 一个用来发送请求的工具方法
     * @param host  服务器
     * @param port  端口号
     * @param request   请求对象
     * @return RpcResponse  响应对象
     */
    public static RpcResponse send(String host, int port, RpcRequest request) {

        // 初始化一个客户端
        NettyRPCClient nettyRpcClient = new NettyRPCClient(host, port);

        // 链接服务器
        nettyRpcClient.connect(nettyRpcClient.getHostname(), nettyRpcClient.getPort());

        // 发送消息
        RpcResponse rpcResponse = nettyRpcClient.send(request);

        nettyRpcClient.close();

        // 拿回结果
        return rpcResponse;
    }
}