package com.yifan.simple.hadoop.codec;


import com.yifan.simple.hadoop.serde.Serialization;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * TODO_MA 自定义编码器
 */
public class RpcEncoder extends MessageToByteEncoder {

    private Class<?> clz;
    private Serialization serialization;

    public RpcEncoder(Class<?> clz, Serialization serialization) {
        this.clz = clz;
        this.serialization = serialization;
    }

    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (clz != null) {
            byte[] bytes = serialization.serialize(msg);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
    }
}