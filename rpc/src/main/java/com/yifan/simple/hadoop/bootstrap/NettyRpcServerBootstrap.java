package com.yifan.simple.hadoop.bootstrap;


import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 一个简单的服务端测试案例
 **/
public class NettyRpcServerBootstrap {
    public static void main(String[] args) {
        new ClassPathXmlApplicationContext("server-spring.xml");
    }
}
