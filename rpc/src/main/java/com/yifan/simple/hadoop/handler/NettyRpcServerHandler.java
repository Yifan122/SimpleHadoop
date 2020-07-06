package com.yifan.simple.hadoop.handler;

import com.yifan.simple.hadoop.protocol.RpcRequest;
import com.yifan.simple.hadoop.protocol.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * RPC Handler（RPC request processor）
 **/
public class NettyRpcServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(NettyRpcServerHandler.class);

    private final Map<String, Object> handlerMap;

    public NettyRpcServerHandler(Map<String, Object> handlerMap) {
        this.handlerMap = handlerMap;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        channelRead0(ctx, msg);
    }

    private void channelRead0(ChannelHandlerContext ctx, Object msg) {

        RpcResponse rpcResponse = new RpcResponse();
        RpcRequest request = (RpcRequest) msg;

        System.out.println("服务端接收到的请求对象：" + request.toString());

        rpcResponse.setRequestId(request.getRequestId());

        try {
            // 收到请求后开始处理请求
            Object result = handler(request);
            rpcResponse.setResult(result);

        } catch (Throwable throwable) {
            // 如果抛出异常也将异常存入 response 中
            rpcResponse.setThrowable(throwable);
            throwable.printStackTrace();
        }
        // 操作完以后写入 netty 的上下文中。netty 自己处理返回值。
        ctx.writeAndFlush(rpcResponse);
    }

    private Object handler(RpcRequest request) throws ClassNotFoundException, InvocationTargetException,
            IllegalAccessException, InstantiationException {
        // 从Spring容器中获得 服务类实例
        Object serviceBean = handlerMap.get(request.getClassName());

        // 通过反射创建 服务类的 方法实例
        Class<?> serviceClass = serviceBean.getClass();
        String methodName = request.getMethodName();

        Class<?>[] parameterTypes = request.getParameterTypes();
        Object[] parameters = request.getParameters();

        // 根本思路还是获取类名和方法名，利用反射实现调用

        // JDK reflect
        /*Method method = serviceClass.getMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(serviceBean, parameters);*/

        FastClass fastClass = FastClass.create(serviceClass);
        FastMethod fastMethod = fastClass.getMethod(methodName, parameterTypes);

        // 实际调用发生的地方
        return fastMethod.invoke(serviceBean, parameters);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
