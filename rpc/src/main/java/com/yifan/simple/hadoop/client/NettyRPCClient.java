package com.yifan.simple.hadoop.client;

import com.yifan.simple.hadoop.codec.RpcDecoder;
import com.yifan.simple.hadoop.codec.RpcEncoder;
import com.yifan.simple.hadoop.handler.NettyRpcClientHandler;
import com.yifan.simple.hadoop.protocol.RpcRequest;
import com.yifan.simple.hadoop.protocol.RpcResponse;
import com.yifan.simple.hadoop.serde.JsonSerialization;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * 定义NettyRPC客户端 NettyRPCClient
 **/
public class NettyRPCClient implements RpcClient{

    private String hostname;
    private int port;

    private Channel channel;
    private NettyRpcClientHandler clientHandler;
    private NioEventLoopGroup eventLoopGroup;

    public NettyRPCClient(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * TODO_MA 发送请求
     * @param request
     * @return
     */
    @Override
    public RpcResponse send(RpcRequest request) {
        try {

            // 发送请求
            channel.writeAndFlush(request).await();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 根据 requestID 来获取 RpcResponse
        return clientHandler.getRpcResponse(request.getRequestId());
    }

    /**
     * TODO_MA 链接服务器
     * @param
     */
    @Override
    public void connect(String hostname, int port) {

        clientHandler = new NettyRpcClientHandler();
        eventLoopGroup = new NioEventLoopGroup();

        Bootstrap clientBootstrap = new Bootstrap();

        clientBootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE,true)
                .option(ChannelOption.TCP_NODELAY,true)
                .handler(initChannelInitializer(clientHandler));

        try {

            // 启动客户端
            channel = clientBootstrap.connect(hostname, port).sync().channel();

        } catch (InterruptedException e) {
            System.out.println("Netty RPC Client start Error !!!! ");
            e.printStackTrace();
        }
    }

    private ChannelInitializer initChannelInitializer(NettyRpcClientHandler clientHandler) {

        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new LengthFieldBasedFrameDecoder(65535,0,4));
                pipeline.addLast(new RpcEncoder(RpcRequest.class,new JsonSerialization()));
                pipeline.addLast(new RpcDecoder(RpcResponse.class,new JsonSerialization()));
                pipeline.addLast(clientHandler);
            }
        };
    }

    /**
     * TODO_MA 关闭服务器
     */
    @Override
    public void close() {
        eventLoopGroup.shutdownGracefully();
        channel.closeFuture().syncUninterruptibly();
    }
}
