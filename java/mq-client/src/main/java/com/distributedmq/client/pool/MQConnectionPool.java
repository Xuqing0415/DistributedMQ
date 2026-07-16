package com.distributedmq.client.pool;

import com.distributedmq.client.codec.MQDecoder;
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

public class MQConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(MQConnectionPool.class);

    private final Map<String, Channel> channelMap = new ConcurrentHashMap<>();
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;

    public MQConnectionPool() {
        this.group = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true);
    }

    public Channel getChannel(String addr) throws InterruptedException {
        Channel channel = channelMap.get(addr);
        if (channel != null && channel.isActive()) {
            return channel;
        }

        synchronized (this) {
            channel = channelMap.get(addr);
            if (channel != null && channel.isActive()) {
                return channel;
            }

            Channel existing = channelMap.get(addr);
            if (existing != null && !existing.isActive()) {
                existing.close();
            }

            return createChannel(addr);
        }
    }

    private Channel createChannel(String addr) throws InterruptedException {
        logger.info("Creating new connection to {}", addr);

        String[] parts = addr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        final CountDownLatch latch = new CountDownLatch(1);
        final Channel[] result = new Channel[1];
        final Exception[] error = new Exception[1];

        Bootstrap newBootstrap = bootstrap.clone()
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new MQDecoder());
                    }
                });

        ChannelFuture future = newBootstrap.connect(host, port).sync();
        future.addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                result[0] = f.channel();
                latch.countDown();
            } else {
                error[0] = new Exception(f.cause());
                latch.countDown();
            }
        });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("Timeout connecting to " + addr);
        }

        if (error[0] != null) {
            throw new RuntimeException("Failed to connect to " + addr, error[0]);
        }

        channelMap.put(addr, result[0]);

        result[0].closeFuture().addListener((ChannelFutureListener) f -> {
            channelMap.remove(addr);
            logger.info("Connection closed: {}", addr);
        });

        return result[0];
    }

    public void invalidateChannel(String addr) {
        Channel channel = channelMap.remove(addr);
        if (channel != null) {
            channel.close();
            logger.info("Invalidated connection: {}", addr);
        }
    }

    public void shutdown() {
        for (Channel channel : channelMap.values()) {
            channel.close();
        }
        group.shutdownGracefully();
        logger.info("Connection pool shutdown complete");
    }
}