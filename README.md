# DistributedMQ

A high-performance distributed message queue system built with C, Go, and Java.

## Features

- **High Throughput**: Designed for 100K+ TPS throughput
- **Raft Consensus**: Built-in Raft implementation for fault tolerance
- **Multi-Language Support**: Go, Java, and C clients
- **Batch Processing**: Built-in batch writing for performance
- **Persistent Storage**: Segment-based commit log with sparse index
- **Dead Letter Queue**: Automatic message retry and DLQ support
- **Delayed Messages**: Time wheel based delayed message delivery
- **Message Tracing**: Full lifecycle message tracking

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Clients                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐│
│  │ Go Client│  │Java Client│  │ C Client│  │ Spring Boot Starter││
│  └────┬────┘  └────┬────┘  └────┬────┘  └─────────┬───────────┘│
└───────┼────────────┼────────────┼──────────────────┼────────────┘
        │            │            │                  │
        ▼            ▼            ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Nameserver                               │
│  - Topic/Partition registration                                 │
│  - Broker discovery and routing                                 │
│  - Leader election coordination                                 │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Broker Cluster                            │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐              │
│  │  Broker 1  │   │  Broker 2  │   │  Broker 3  │              │
│  │  (Leader)  │◄─►│  (Follower)│◄─►│  (Follower)│              │
│  ├────────────┤   ├────────────┤   ├────────────┤              │
│  │  Raft Node │   │  Raft Node │   │  Raft Node │              │
│  │  CommitLog │   │  CommitLog │   │  CommitLog │              │
│  │  SparseIdx │   │  SparseIdx │   │  SparseIdx │              │
│  │  TimeWheel │   │  TimeWheel │   │  TimeWheel │              │
│  └────────────┘   └────────────┘   └────────────┘              │
│                         Raft Consensus                          │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.20+
- Java 11+
- GCC 9+

### Build

```bash
# Build Go components
cd cluster/go
go build -o broker ./broker
go build -o nameserver ./nameserver

# Build C storage engine (optional)
cd cluster/c
make

# Build Java client
cd java/mq-client
mvn clean package
```

### Run

```bash
# Start nameserver
./nameserver -addr localhost:8080

# Start 3 brokers (Raft cluster)
./broker -addr localhost:9092 -nameserver localhost:8080 -node-id 1
./broker -addr localhost:9093 -nameserver localhost:8080 -node-id 2
./broker -addr localhost:9094 -nameserver localhost:8080 -node-id 3
```

### Send Messages (Go)

```bash
cd samples/go-producer
go run main.go -broker localhost:9092 -topic test -count 1000 -batch 100
```

### Send Messages (Java)

```java
MQProducer producer = new MQProducer("localhost:9092");
producer.send("test", 0, "Hello DistributedMQ".getBytes(), 1);
producer.close();
```

## Performance

| Configuration | Throughput | Latency |
|--------------|-----------|---------|
| acks=0, batch=100 | ~100K msg/s | <1ms |
| acks=1, batch=100 | ~50K msg/s | <5ms |
| acks=-1, batch=100 | ~30K msg/s | <10ms |

## Documentation

- [Architecture Decision Record](docs/architecture.md)
- [Raft Implementation Guide](docs/raft.md)
- [Storage Engine Design](docs/storage.md)
- [API Reference](docs/api.md)

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## Acknowledgments

- Inspired by Kafka and RocketMQ
- Raft implementation based on "In Search of an Understandable Consensus Algorithm"