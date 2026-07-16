package com.distributedmq.client.codec;

import com.distributedmq.client.model.MQMessage;
import com.distributedmq.client.model.MQCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.ArrayList;
import java.util.List;

public class MQDecoder extends ByteToMessageDecoder {

    public static class FetchResponse {
        private byte status;
        private long highWatermark;
        private int messageCount;
        private List<MQMessage> messages;
        private String redirectLeader;

        public byte getStatus() { return status; }
        public void setStatus(byte status) { this.status = status; }
        public long getHighWatermark() { return highWatermark; }
        public void setHighWatermark(long highWatermark) { this.highWatermark = highWatermark; }
        public int getMessageCount() { return messageCount; }
        public void setMessageCount(int messageCount) { this.messageCount = messageCount; }
        public List<MQMessage> getMessages() { return messages; }
        public void setMessages(List<MQMessage> messages) { this.messages = messages; }
        public String getRedirectLeader() { return redirectLeader; }
        public void setRedirectLeader(String redirectLeader) { this.redirectLeader = redirectLeader; }
        public boolean isSuccess() { return status == MQCommand.RESP_SUCCESS; }
        public boolean isRedirect() { return status == MQCommand.RESP_REDIRECT; }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 2) {
            return;
        }

        in.markReaderIndex();
        byte magic = in.readByte();
        if (magic != MQCommand.MAGIC) {
            in.resetReaderIndex();
            return;
        }

        byte status = in.readByte();

        if (status == MQCommand.RESP_SUCCESS) {
            if (in.readableBytes() < 12) {
                in.resetReaderIndex();
                return;
            }

            long highWatermark = in.readLong();
            int msgCount = in.readInt();

            FetchResponse response = new FetchResponse();
            response.setStatus(status);
            response.setHighWatermark(highWatermark);
            response.setMessageCount(msgCount);

            List<MQMessage> messages = new ArrayList<>(msgCount);
            for (int i = 0; i < msgCount; i++) {
                if (in.readableBytes() < 20) {
                    in.resetReaderIndex();
                    return;
                }

                long offset = in.readLong();
                long timestamp = in.readLong();
                int dataLen = in.readInt();

                if (in.readableBytes() < dataLen) {
                    in.resetReaderIndex();
                    return;
                }

                byte[] data = new byte[dataLen];
                in.readBytes(data);

                MQMessage message = new MQMessage();
                message.setOffset(offset);
                message.setTimestamp(timestamp);
                message.setData(data);
                messages.add(message);
            }
            response.setMessages(messages);
            out.add(response);

        } else if (status == MQCommand.RESP_REDIRECT) {
            if (in.readableBytes() < 1) {
                in.resetReaderIndex();
                return;
            }

            int leaderLen = in.readByte() & 0xFF;
            if (in.readableBytes() < leaderLen) {
                in.resetReaderIndex();
                return;
            }

            byte[] leaderBytes = new byte[leaderLen];
            in.readBytes(leaderBytes);

            FetchResponse response = new FetchResponse();
            response.setStatus(status);
            response.setRedirectLeader(new String(leaderBytes));
            out.add(response);

        } else {
            FetchResponse response = new FetchResponse();
            response.setStatus(status);
            out.add(response);
        }
    }
}