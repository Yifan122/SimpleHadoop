package com.yifan.simple.hadoop.codec;


import com.yifan.simple.hadoop.serde.Serialization;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * TODO_MA 自定义解码器
 */
public class RpcDecoder extends ByteToMessageDecoder {

    private Class<?> clz;
    private Serialization serialization;

    public RpcDecoder(Class<?> clz, Serialization serialization) {
        this.clz = clz;
        this.serialization = serialization;
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }

        in.markReaderIndex();
        int dataLength = in.readInt();
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] data = new byte[dataLength];
        in.readBytes(data);

        Object obj = serialization.deSerialize(data, clz);
        out.add(obj);
    }
}