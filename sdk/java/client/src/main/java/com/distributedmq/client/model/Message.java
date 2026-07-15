package com.distributedmq.client.model;

import java.nio.charset.StandardCharsets;

public class Message {
    private String topic;
    private int partition;
    private String key;
    private byte[] value;
    private String valueBase64;
    private long offset;
    private long timestamp;

    public Message() {}

    public Message(String topic, int partition, String key, byte[] value, long offset, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public int getPartition() { return partition; }
    public void setPartition(int partition) { this.partition = partition; }
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    public byte[] getValue() { 
        if (value == null && valueBase64 != null) {
            value = java.util.Base64.getDecoder().decode(valueBase64);
        }
        return value; 
    }
    public void setValue(byte[] value) { this.value = value; }
    public String getValueBase64() { 
        if (valueBase64 == null && value != null) {
            valueBase64 = java.util.Base64.getEncoder().encodeToString(value);
        }
        return valueBase64; 
    }
    public void setValueBase64(String valueBase64) { this.valueBase64 = valueBase64; }
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
