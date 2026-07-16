package com.distributedmq.client.codec;

import com.distributedmq.client.model.MQCommand;
import com.distributedmq.client.model.MQMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MQEncoder extends MessageToByteEncoder<MQMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, MQMessage msg, ByteBuf out) throws Exception {
        byte[] topicBytes = msg.getTopic().getBytes();
        byte[] data = msg.getData() != null ? msg.getData() : new byte[0];

        out.writeByte(MQCommand.MAGIC);
        out.writeByte(MQCommand.CMD_PRODUCE);
        out.writeShort(topicBytes.length);
        out.writeInt(msg.getPartition());
        out.writeInt(data.length);
        out.writeBytes(topicBytes);
        out.writeBytes(data);
    }
}