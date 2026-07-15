package com.distributedmq.client;

import com.distributedmq.client.model.Message;
import com.distributedmq.client.model.RouteInfo;
import com.distributedmq.client.netty.MQNettyClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MQClient {

    private final MQNettyClient nettyClient;
    private final NameServerClient nameServerClient;

    public MQClient(String nameserverAddr) {
        this.nameServerClient = new NameServerClient(nameserverAddr);
        this.nettyClient = new MQNettyClient();
    }

    public CompletableFuture<Message> produce(String topic, String key, byte[] value) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                RouteInfo route = nameServerClient.getRoute(topic, key);
                return nettyClient.send(route.getLeader(), topic, key, value);
            } catch (Exception e) {
                throw new RuntimeException("Produce failed", e);
            }
        });
    }

    public CompletableFuture<List<Message>> consume(String topic, String key, long offset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                RouteInfo route = nameServerClient.getRoute(topic, key);
                return nettyClient.consume(route.getLeader(), topic, route.getPartition(), offset);
            } catch (Exception e) {
                throw new RuntimeException("Consume failed", e);
            }
        });
    }

    public void createTopic(String topic, int partitions) throws Exception {
        nameServerClient.createTopic(topic, partitions);
    }

    public void close() {
        nettyClient.close();
    }
}
