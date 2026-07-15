package com.distributedmq.client.netty;

import com.distributedmq.client.model.Message;
import com.google.gson.Gson;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MQNettyClient {

    private EventLoopGroup group;
    private final Gson gson;
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();

    public MQNettyClient() {
        this.gson = new Gson();
        this.group = new NioEventLoopGroup();
    }

    public Message send(String brokerAddr, String topic, String key, byte[] value) throws Exception {
        Channel channel = getChannel(brokerAddr);

        String requestId = UUID.randomUUID().toString();
        ResponseHandler handler = (ResponseHandler) channel.pipeline().get("responseHandler");
        handler.registerRequest(requestId);

        String valueBase64 = java.util.Base64.getEncoder().encodeToString(value);
        Map<String, Object> request = Map.of(
            "requestId", requestId,
            "type", "produce",
            "topic", topic,
            "key", key,
            "value", valueBase64
        );

        String json = gson.toJson(request) + "\n";
        channel.writeAndFlush(json).sync();

        return handler.getResponse(requestId, 5000);
    }

    public List<Message> consume(String brokerAddr, String topic, int partition, long offset) throws Exception {
        Channel channel = getChannel(brokerAddr);

        String requestId = UUID.randomUUID().toString();
        ResponseHandler handler = (ResponseHandler) channel.pipeline().get("responseHandler");
        handler.registerRequest(requestId);

        Map<String, Object> request = Map.of(
            "requestId", requestId,
            "type", "consume",
            "topic", topic,
            "partition", partition,
            "offset", offset
        );

        String json = gson.toJson(request) + "\n";
        channel.writeAndFlush(json).sync();

        Message response = handler.getResponse(requestId, 5000);
        return response != null ? Collections.singletonList(response) : Collections.emptyList();
    }

    private Channel getChannel(String brokerAddr) throws Exception {
        if (channels.containsKey(brokerAddr)) {
            Channel channel = channels.get(brokerAddr);
            if (channel.isActive()) {
                return channel;
            }
            channels.remove(brokerAddr);
        }

        String[] parts = brokerAddr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        CountDownLatch latch = new CountDownLatch(1);
        Channel[] channelRef = new Channel[1];

        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.SO_KEEPALIVE, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) throws Exception {
                 ch.pipeline().addLast(
                     new DelimiterBasedFrameDecoder(1024 * 1024, Unpooled.copiedBuffer("\n".getBytes())),
                     new StringDecoder(),
                     new StringEncoder(),
                     new ResponseHandler(latch)
                 );
             }
         });

        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                channelRef[0] = future.channel();
                channels.put(brokerAddr, channelRef[0]);
            }
            latch.countDown();
        });

        latch.await(5, TimeUnit.SECONDS);
        if (channelRef[0] == null) {
            throw new Exception("Failed to connect to broker: " + brokerAddr);
        }

        return channelRef[0];
    }

    public void close() {
        for (Channel channel : channels.values()) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully();
    }

    private static class ResponseHandler extends SimpleChannelInboundHandler<String> {
        private final CountDownLatch latch;
        private final Map<String, Message> responses = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> latches = new ConcurrentHashMap<>();

        public ResponseHandler(CountDownLatch latch) {
            this.latch = latch;
        }

        public void registerRequest(String requestId) {
            latches.put(requestId, new CountDownLatch(1));
        }

        public Message getResponse(String requestId, long timeout) throws Exception {
            CountDownLatch responseLatch = latches.get(requestId);
            if (responseLatch == null) {
                throw new Exception("Request not registered: " + requestId);
            }
            try {
                responseLatch.await(timeout, TimeUnit.MILLISECONDS);
                return responses.remove(requestId);
            } finally {
                latches.remove(requestId);
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
            Gson gson = new Gson();
            try {
                Map<String, Object> map = gson.fromJson(msg, Map.class);
                String requestId = (String) map.get("requestId");
                if (requestId != null) {
                    Message message = gson.fromJson(msg, Message.class);
                    responses.put(requestId, message);
                    CountDownLatch responseLatch = latches.remove(requestId);
                    if (responseLatch != null) {
                        responseLatch.countDown();
                    }
                }
            } catch (Exception e) {
                Message message = gson.fromJson(msg, Message.class);
                if (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        }
    }
}
