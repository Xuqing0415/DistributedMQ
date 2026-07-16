package com.distributedmq.client;

import com.distributedmq.client.codec.MQDecoder;
import com.distributedmq.client.model.MQCommand;
import com.distributedmq.client.pool.MQConnectionPool;
import io.netty.buffer.ByteBuf;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MQProducer {
    private static final Logger logger = LoggerFactory.getLogger(MQProducer.class);

    private final String nameserverURL;
    private final MQConnectionPool connectionPool;
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;
    private final Map<Channel, MQResponseHandler> responseHandlers = new ConcurrentHashMap<>();

    public MQProducer(String nameserverURL) {
        this.nameserverURL = nameserverURL;
        this.connectionPool = new MQConnectionPool();
        this.group = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true);
    }

    public void send(String topic, int partition, byte[] data) throws Exception {
        send(topic, partition, data, 1, 3);
    }

    public void send(String topic, int partition, byte[] data, int acks, int maxRetries) throws Exception {
        String brokerAddr = lookupBroker(topic, partition);
        Exception lastError = null;

        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                Channel channel = connectionPool.getChannel(brokerAddr);
                MQDecoder.FetchResponse response = doSend(channel, topic, partition, data, acks);

                if (response.isSuccess()) {
                    return;
                } else if (response.isRedirect()) {
                    brokerAddr = response.getRedirectLeader();
                    connectionPool.invalidateChannel(brokerAddr);
                    logger.info("Redirected to leader: {}", brokerAddr);
                    continue;
                } else {
                    throw new RuntimeException("Send failed, status: " + response.getStatus());
                }
            } catch (Exception e) {
                lastError = e;
                logger.warn("Send attempt {} failed: {}", retry + 1, e.getMessage());
                connectionPool.invalidateChannel(brokerAddr);
                Thread.sleep(100 * (retry + 1));
            }
        }

        throw new RuntimeException("Send failed after " + maxRetries + " retries", lastError);
    }

    private MQDecoder.FetchResponse doSend(Channel channel, String topic, int partition, byte[] data, int acks) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final MQDecoder.FetchResponse[] result = new MQDecoder.FetchResponse[1];

        MQResponseHandler handler = responseHandlers.get(channel);
        if (handler == null) {
            handler = new MQResponseHandler();
            responseHandlers.put(channel, handler);
            channel.pipeline().addLast(handler);

            channel.closeFuture().addListener(f -> {
                responseHandlers.remove(channel);
            });
        }

        byte[] topicBytes = topic.getBytes();
        byte[] fullData = new byte[1 + data.length];
        fullData[0] = (byte) acks;
        System.arraycopy(data, 0, fullData, 1, data.length);

        ByteBuf buf = channel.alloc().buffer(12 + topicBytes.length + fullData.length);
        try {
            buf.writeByte(MQCommand.MAGIC);
            buf.writeByte(MQCommand.CMD_PRODUCE);
            buf.writeShort(topicBytes.length);
            buf.writeInt(partition);
            buf.writeInt(fullData.length);
            buf.writeBytes(topicBytes);
            buf.writeBytes(fullData);

            handler.setPendingResponse(result, latch);

            ChannelFuture future = channel.writeAndFlush(buf);
            future.addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                    latch.countDown();
                    logger.warn("Write failed: {}", f.cause().getMessage());
                }
            });

            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("Send timeout");
            }

            if (result[0] == null) {
                throw new RuntimeException("No response received");
            }

            return result[0];
        } finally {
            buf.release();
        }
    }

    private String lookupBroker(String topic, int partition) throws Exception {
        java.net.URL url = new java.net.URL(nameserverURL + "/route?topic=" + topic + "&partition=" + partition);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try {
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed to lookup broker: HTTP " + conn.getResponseCode());
            }

            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream()));
            try {
                String line = reader.readLine();
                com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                com.google.gson.JsonObject obj = parser.parse(line).getAsJsonObject();
                return obj.get("leader").getAsString();
            } finally {
                reader.close();
            }
        } finally {
            conn.disconnect();
        }
    }

    public void close() {
        connectionPool.shutdown();
        group.shutdownGracefully();
        responseHandlers.clear();
        logger.info("Producer closed");
    }

    private static class MQResponseHandler extends SimpleChannelInboundHandler<MQDecoder.FetchResponse> {
        private volatile MQDecoder.FetchResponse[] pendingResult;
        private volatile CountDownLatch pendingLatch;

        public synchronized void setPendingResponse(MQDecoder.FetchResponse[] result, CountDownLatch latch) {
            this.pendingResult = result;
            this.pendingLatch = latch;
        }

        @Override
        protected synchronized void channelRead0(ChannelHandlerContext ctx, MQDecoder.FetchResponse msg) {
            if (pendingResult != null && pendingLatch != null) {
                pendingResult[0] = msg;
                pendingLatch.countDown();
                pendingResult = null;
                pendingLatch = null;
            }
        }

        @Override
        public synchronized void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (pendingLatch != null) {
                pendingLatch.countDown();
                pendingLatch = null;
            }
            ctx.close();
        }
    }
}