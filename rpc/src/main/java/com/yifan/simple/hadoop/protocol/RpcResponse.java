package com.yifan.simple.hadoop.protocol;

/**
 * NettyRpc 的 客户端发起请求的 响应对象
 **/
public class RpcResponse {

    // 调用编号
    private String requestId;

    // 抛出的异常
    private Throwable throwable;

    // 返回结果
    private Object result;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }
}
