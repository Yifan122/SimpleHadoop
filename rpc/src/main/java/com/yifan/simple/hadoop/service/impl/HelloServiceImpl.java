package com.yifan.simple.hadoop.service.impl;

import com.yifan.simple.hadoop.server.RpcService;
import com.yifan.simple.hadoop.service.HelloService;

/**
 * 一个简单的服务实现
 **/
@RpcService(HelloService.class)
public class HelloServiceImpl implements HelloService {

    public HelloServiceImpl() {
    }

    @Override
    public String hello(String name) {
        return "Server: Hello " + name;
    }
}
