package com.yifan.simple.hadoop.client;


import com.yifan.simple.hadoop.protocol.RpcRequest;
import com.yifan.simple.hadoop.protocol.RpcResponse;

/**
 * TODO_MA 定义一个客户端的接口
 */
public interface RpcClient {

    // TODO_MA 发送请求
    RpcResponse send(RpcRequest request);

    // TODO_MA 链接服务器
    void connect(String hostname, int port);

    // TODO_MA 关闭客户端
    void close();
}