package com.yifan.simple.hadoop.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;

/**
 *
 **/
public interface RpcServer {

    void serverStart();

    ServerBootstrap initServerBootstrap();

    ChannelInitializer initChannelInitializer();


}
