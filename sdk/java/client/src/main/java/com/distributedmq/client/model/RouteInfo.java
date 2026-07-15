package com.distributedmq.client.model;

import java.util.List;

public class RouteInfo {
    private String topic;
    private int partition;
    private String leader;
    private List<String> replicas;
    private List<String> isr;

    public RouteInfo() {}

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public int getPartition() { return partition; }
    public void setPartition(int partition) { this.partition = partition; }
    public String getLeader() { return leader; }
    public void setLeader(String leader) { this.leader = leader; }
    public List<String> getReplicas() { return replicas; }
    public void setReplicas(List<String> replicas) { this.replicas = replicas; }
    public List<String> getIsr() { return isr; }
    public void setIsr(List<String> isr) { this.isr = isr; }
}
