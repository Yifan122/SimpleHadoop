package com.yifan.simple.hadoop.common;

import com.yifan.simple.hadoop.protocol.RpcRequest;
import com.yifan.simple.hadoop.protocol.RpcResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;

/**
 * TODO_MA 代理
 */
public class RpcInvocationHandler<T> implements InvocationHandler {

    private String host;
    private int port;
    private Class<T> clazz;

    public RpcInvocationHandler(String host, int port, Class<T> clazz) {
        this.clazz = clazz;
        this.host = host;
        this.port = port;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        /**
         * 构建 RpcRequest 对象
         */
        RpcRequest request = new RpcRequest();

        // 获取参数
        String requestId = UUID.randomUUID().toString();
        String className = method.getDeclaringClass().getName();
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();

        /**
         * 组装 RpcRequest 请求对象
         */
        request.setRequestId(requestId);
        request.setClassName(className);
        request.setMethodName(methodName);
        request.setParameterTypes(parameterTypes);
        request.setParameters(args);

        System.out.println(request.toString());
        /**
         * 发送请求
         */
        RpcResponse rpcResponse = Sender.send(host, port, request);
        return rpcResponse.getResult();
    }
}