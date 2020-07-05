package com.yifan.simple.hadoop.common;


import com.yifan.simple.hadoop.protocol.RpcResponse;

/**
 * TODO_MA 定义了一个用来管理 Response 的类
 *  通过异步的方式获取响应结果
 */
public class RpcFuture {

    // 响应结果
    private RpcResponse rpcResponse;

    // 是否已经能获取到结果
    private volatile boolean isResponseSet = false;

    // 锁对象
    private final Object lock = new Object();

    /**
     * @param timeout
     * @return
     * 获取结果
     */
    public RpcResponse getResponse(int timeout) {
        synchronized (lock) {
            while (!isResponseSet) {
                try {
                    //wait
                    lock.wait(timeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return rpcResponse;
        }
    }

    /**
     * TODO_MA 设置结果
     * @param response
     */
    public void setResponse(RpcResponse response) {
        if (isResponseSet) {
            return;
        }
        synchronized (lock) {
            this.rpcResponse = response;
            this.isResponseSet = true;
            //notiy
            lock.notify();
        }
    }
}