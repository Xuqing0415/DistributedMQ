package com.distributedmq.client.example;

import com.distributedmq.client.MQProducer;

public class ProducerExample {
    public static void main(String[] args) throws Exception {
        String nameserverURL = "http://localhost:9090";
        String topic = "test";
        int count = 100;
        int acks = 1;

        if (args.length > 0) {
            nameserverURL = args[0];
        }
        if (args.length > 1) {
            topic = args[1];
        }
        if (args.length > 2) {
            count = Integer.parseInt(args[2]);
        }
        if (args.length > 3) {
            acks = Integer.parseInt(args[3]);
        }

        MQProducer producer = new MQProducer(nameserverURL);

        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                String message = "Message " + i + ": Hello DistributedMQ from Java!";
                producer.send(topic, i % 4, message.getBytes(), acks, 3);

                if (i > 0 && i % 10 == 0) {
                    System.out.println("Sent " + (i + 1) + "/" + count + " messages");
                }
            }

            long duration = System.currentTimeMillis() - start;
            double tps = count * 1000.0 / duration;
            System.out.println("\n=== Results ===");
            System.out.println("Total messages: " + count);
            System.out.println("Duration: " + duration + " ms");
            System.out.println("Throughput: " + String.format("%.2f", tps) + " msg/s");
        } finally {
            producer.close();
        }
    }
}