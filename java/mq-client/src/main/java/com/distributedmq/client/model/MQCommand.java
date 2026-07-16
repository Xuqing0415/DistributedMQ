package com.distributedmq.client.model;

public class MQCommand {
    public static final byte MAGIC = (byte) 0xD0;

    public static final byte CMD_PRODUCE = 0x01;
    public static final byte CMD_FETCH = 0x02;
    public static final byte CMD_JOIN_GROUP = 0x03;
    public static final byte CMD_HEARTBEAT = 0x04;
    public static final byte CMD_COMMIT_OFFSET = 0x05;
    public static final byte CMD_LEAVE_GROUP = 0x06;
    public static final byte CMD_ACK = 0x07;

    public static final byte RESP_SUCCESS = 0x00;
    public static final byte RESP_REDIRECT = 0x01;
    public static final byte RESP_ERROR = (byte) 0xFF;
    public static final byte RESP_DLQ = 0x02;
    public static final byte RESP_RETRIED = 0x03;
}