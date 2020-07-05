package com.yifan.simple.hadoop.serde;

/**
 * TODO_MA 定义序列化机制
 *
 * 序列化机制有很多：
 * jdk 的序列化方法。（不推荐，不利于之后的跨语言调用）
 * json 可读性强，但是序列化速度慢，体积大。
 * protobuf，kyro，Hessian 等都是优秀的序列化框架，也可按需选择。
 *
 * 为了简单和便于调试，我们就选择 json 作为序列化协议，使用jackson作为 json 解析框架。
 */
public interface Serialization {

    // 序列化方法
    <T> byte[] serialize(T obj);

    // 反序列化方法
    <T> T deSerialize(byte[] data, Class<T> clz);
}