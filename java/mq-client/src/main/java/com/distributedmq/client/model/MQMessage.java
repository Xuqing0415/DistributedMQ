package com.distributedmq.client.model;

public class MQMessage {
    private String topic;
    private int partition;
    private byte[] data;
    private long offset;
    private long timestamp;

    public MQMessage() {
    }

    public MQMessage(String topic, int partition, byte[] data) {
        this.topic = topic;
        this.partition = partition;
        this.data = data;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDataAsString() {
        return data != null ? new String(data) : null;
    }
}