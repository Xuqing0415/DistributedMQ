package com.distributedmq.client;

import com.distributedmq.client.model.RouteInfo;
import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class NameServerClient {

    private final String nameserverAddr;
    private final OkHttpClient httpClient;
    private final Gson gson;

    public NameServerClient(String nameserverAddr) {
        this.nameserverAddr = nameserverAddr;
        this.httpClient = new OkHttpClient();
        this.gson = new Gson();
    }

    public RouteInfo getRoute(String topic, String key) throws Exception {
        String url = String.format("http://%s/route?topic=%s&key=%s", nameserverAddr, topic, key);
        Request request = new Request.Builder().url(url).build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new Exception("Failed to get route: " + response.code());
            }
            String body = response.body().string();
            return gson.fromJson(body, RouteInfo.class);
        }
    }

    public void createTopic(String topic, int partitions) throws Exception {
        String url = String.format("http://%s/topic/create?topic=%s&partitions=%d", nameserverAddr, topic, partitions);
        Request request = new Request.Builder().url(url).build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new Exception("Failed to create topic: " + response.code());
            }
        }
    }
}
