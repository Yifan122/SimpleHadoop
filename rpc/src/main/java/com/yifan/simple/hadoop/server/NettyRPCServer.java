package com.yifan.simple.hadoop.server;

import com.yifan.simple.hadoop.codec.RpcDecoder;
import com.yifan.simple.hadoop.codec.RpcEncoder;
import com.yifan.simple.hadoop.common.NettyProperties;
import com.yifan.simple.hadoop.handler.NettyRpcServerHandler;
import com.yifan.simple.hadoop.protocol.RpcRequest;
import com.yifan.simple.hadoop.protocol.RpcResponse;
import com.yifan.simple.hadoop.serde.JsonSerialization;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.HashMap;
import java.util.Map;

/**
 * 定义NettyRPC服务端 NettyRPCServer
 **/
public class NettyRPCServer implements RpcServer, ApplicationContextAware, InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(NettyRPCServer.class);

    private String hostname;
    private int port;

    private Map<String, Object> handlerMap = new HashMap<>();

    public NettyRPCServer(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public NettyRPCServer() {
        this.hostname = NettyProperties.REMOTE_HOST;
        this.port = NettyProperties.PORT;
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        Map<String, Object> serviceBeanMap = ctx.getBeansWithAnnotation(RpcService.class);
        if (serviceBeanMap.size() > 0) {
            for (Object serviceBean : serviceBeanMap.values()) {
                String interfaceName = serviceBean.getClass().getAnnotation(RpcService.class).value().getName();
                logger.info("Loading service: {}", interfaceName);
                handlerMap.put(interfaceName, serviceBean);
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        serverStart();
    }

    @Override
    public void serverStart() {
        serverStart0();
    }

    private void serverStart0() {
        ServerBootstrap serverBootstrap = initServerBootstrap();
        startServerBootstrap(serverBootstrap);
        System.out.println("serve start ");
    }

    // 启动服务器
    private void startServerBootstrap(ServerBootstrap serverBootstrap) {
        try {
            serverBootstrap.bind(port).sync().channel().closeFuture().channel();
        } catch (InterruptedException e) {
            System.out.println("服务启动异常！！！！");
            e.printStackTrace();
        }
    }

    @Override
    public ServerBootstrap initServerBootstrap() {

        // 服务端启动类
        ServerBootstrap serverBootstrap = new ServerBootstrap();

        // 服务端的两组工作线程。
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(4);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        ChannelInitializer serverInitializer = initChannelInitializer();

        // 给启动类 ServerBootstrap 配置一些参数
        serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(serverInitializer)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        return serverBootstrap;
    }

    /**
     * 构建 ChannelInitializer
     */
    @Override
    public ChannelInitializer initChannelInitializer() {
        ChannelInitializer serverInitializer = new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel sc) throws Exception {
                ChannelPipeline pipeline = sc.pipeline();
                // 处理 tcp 请求中粘包的 coder，具体作用可以自行 google
                pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 4));

                // protocol 中实现的 序列化和反序列化 coder
                pipeline.addLast(new RpcEncoder(RpcResponse.class, new JsonSerialization()));
                pipeline.addLast(new RpcDecoder(RpcRequest.class, new JsonSerialization()));

                // 具体处理请求的 handler 下文具体解释
                pipeline.addLast(new NettyRpcServerHandler(handlerMap));
            }
        };

        return serverInitializer;
    }
}
