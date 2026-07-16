package com.distributedmq.client;

import com.distributedmq.client.codec.MQDecoder;
import com.distributedmq.client.model.MQCommand;
import com.distributedmq.client.model.MQMessage;
import com.distributedmq.client.pool.MQConnectionPool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MQConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MQConsumer.class);

    private final String nameserverURL;
    private final String groupID;
    private final String clientID;
    private final String topic;
    private final MQConnectionPool connectionPool;

    private final Map<Integer, Long> offsets = new ConcurrentHashMap<>();
    private final List<Integer> assignments = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;
    private Thread heartbeatThread;
    private final Map<Channel, MQResponseHandler> responseHandlers = new ConcurrentHashMap<>();

    public MQConsumer(String nameserverURL, String groupID, String clientID, String topic) {
        this.nameserverURL = nameserverURL;
        this.groupID = groupID;
        this.clientID = clientID;
        this.topic = topic;
        this.connectionPool = new MQConnectionPool();
    }

    public void joinGroup() throws Exception {
        java.net.URL url = new java.net.URL(nameserverURL + "/consumer/join?group_id=" + groupID + "&client_id=" + clientID + "&topic=" + topic);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try {
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Join group failed: HTTP " + conn.getResponseCode());
            }

            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream()));
            try {
                String line = reader.readLine();
                com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                com.google.gson.JsonObject obj = parser.parse(line).getAsJsonObject();

                com.google.gson.JsonArray partitions = obj.getAsJsonArray("assignments");
                assignments.clear();
                for (int i = 0; i < partitions.size(); i++) {
                    int partition = partitions.get(i).getAsInt();
                    assignments.add(partition);
                    offsets.put(partition, 0L);
                }
            } finally {
                reader.close();
            }
        } finally {
            conn.disconnect();
        }

        logger.info("Joined group {}, assigned partitions: {}", groupID, assignments);

        startHeartbeat();
    }

    public List<MQMessage> poll(long timeoutMs) throws Exception {
        if (!running) {
            return Collections.emptyList();
        }

        List<MQMessage> allMessages = new ArrayList<>();

        for (Integer partition : assignments) {
            String brokerAddr = lookupBroker(topic, partition);
            Long offset = offsets.get(partition);
            if (offset == null) {
                offset = getOffsetFromNameserver(partition);
                offsets.put(partition, offset);
            }

            try {
                Channel channel = connectionPool.getChannel(brokerAddr);
                MQDecoder.FetchResponse response = doFetch(channel, topic, partition, offset, 1024 * 1024);

                if (response.isSuccess()) {
                    for (MQMessage msg : response.getMessages()) {
                        msg.setTopic(topic);
                        msg.setPartition(partition);
                        allMessages.add(msg);
                    }

                    if (response.getMessages().size() > 0) {
                        long lastOffset = response.getMessages().get(response.getMessages().size() - 1).getOffset();
                        offsets.put(partition, lastOffset + 1);
                    }

                    long lag = response.getHighWatermark() - offsets.get(partition);
                    logger.debug("Partition {}: offset={}, highWatermark={}, lag={}", partition, offsets.get(partition), response.getHighWatermark(), lag);

                } else if (response.isRedirect()) {
                    connectionPool.invalidateChannel(brokerAddr);
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch from partition {}: {}", partition, e.getMessage());
                connectionPool.invalidateChannel(brokerAddr);
            }
        }

        return allMessages;
    }

    public void commitOffset(int partition, long offset) throws Exception {
        java.net.URL url = new java.net.URL(nameserverURL + "/consumer/commit_offset?group_id=" + groupID + "&client_id=" + clientID + "&topic=" + topic + "&partition=" + partition + "&offset=" + offset);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try {
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Commit offset failed: HTTP " + conn.getResponseCode());
            }
        } finally {
            conn.disconnect();
        }

        offsets.put(partition, offset);
        logger.debug("Committed offset {} for partition {}", offset, partition);
    }

    public void leaveGroup() throws Exception {
        running = false;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            heartbeatThread.join();
        }

        java.net.URL url = new java.net.URL(nameserverURL + "/consumer/leave?group_id=" + groupID + "&client_id=" + clientID);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try {
            if (conn.getResponseCode() != 200) {
                logger.warn("Leave group failed: HTTP {}", conn.getResponseCode());
            }
        } finally {
            conn.disconnect();
        }

        connectionPool.shutdown();
        responseHandlers.clear();
        logger.info("Left group {}", groupID);
    }

    private void startHeartbeat() {
        heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    java.net.URL url = new java.net.URL(nameserverURL + "/consumer/heartbeat?group_id=" + groupID + "&client_id=" + clientID);
                    java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("GET");
                    conn.setConnectTimeout(5000);
                    conn.setReadTimeout(5000);

                    try {
                        conn.getResponseCode();
                    } finally {
                        conn.disconnect();
                    }

                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.warn("Heartbeat failed: {}", e.getMessage());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        heartbeatThread.start();
        logger.info("Heartbeat thread started");
    }

    private MQDecoder.FetchResponse doFetch(Channel channel, String topic, int partition, long offset, int maxBytes) throws InterruptedException {
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
        byte[] data = new byte[12];
        java.nio.ByteBuffer.wrap(data, 0, 8).putLong(offset);
        java.nio.ByteBuffer.wrap(data, 8, 4).putInt(maxBytes);

        ByteBuf buf = channel.alloc().buffer(12 + topicBytes.length + data.length);
        try {
            buf.writeByte(MQCommand.MAGIC);
            buf.writeByte(MQCommand.CMD_FETCH);
            buf.writeShort(topicBytes.length);
            buf.writeInt(partition);
            buf.writeInt(data.length);
            buf.writeBytes(topicBytes);
            buf.writeBytes(data);

            handler.setPendingResponse(result, latch);
            channel.writeAndFlush(buf);

            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("Fetch timeout");
            }

            if (result[0] == null) {
                throw new RuntimeException("Fetch timeout");
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

    private long getOffsetFromNameserver(int partition) throws Exception {
        java.net.URL url = new java.net.URL(nameserverURL + "/consumer/offset?group_id=" + groupID + "&topic=" + topic + "&partition=" + partition);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try {
            if (conn.getResponseCode() == 200) {
                java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream()));
                try {
                    String line = reader.readLine();
                    com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                    com.google.gson.JsonObject obj = parser.parse(line).getAsJsonObject();
                    return obj.get("offset").getAsLong();
                } finally {
                    reader.close();
                }
            }
        } finally {
            conn.disconnect();
        }

        return 0L;
    }

    public List<Integer> getAssignments() {
        return new ArrayList<>(assignments);
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