# DistributedMQ 性能测试报告

## 测试环境

| 项目 | 配置 |
|------|------|
| 操作系统 | Windows 10 |
| CPU | 4 核 |
| 内存 | 8GB |
| Go 版本 | 1.22+ |
| 部署模式 | 单节点（开发环境） |

## 压测结果

### 场景 1：acks=1（高性能模式）

```bash
go run main.go -broker localhost:9092 -topic benchmark -count 1000 -acks 1
```

| 指标 | 结果 |
|------|------|
| 总消息数 | 1000 |
| 成功数 | 1000 |
| 失败数 | 0 |
| 重定向数 | 0 |
| 耗时 | 9.77 秒 |
| **吞吐量** | **102.4 msg/s** |

### 场景 2：acks=-1（强一致性模式）

```bash
go run main.go -broker localhost:9092 -topic benchmark -count 1000 -acks -1
```

| 指标 | 结果 |
|------|------|
| 总消息数 | 1000 |
| 成功数 | 1000 |
| 失败数 | 0 |
| 重定向数 | 0 |
| 耗时 | 9.80 秒 |
| **吞吐量** | **102.0 msg/s** |

### 场景 3：多分区并发

```bash
# 3 个分区同时发送
go run main.go -broker localhost:9092 -topic benchmark -count 1000 -partition 0 -acks 1
go run main.go -broker localhost:9092 -topic benchmark -count 1000 -partition 1 -acks 1
go run main.go -broker localhost:9092 -topic benchmark -count 1000 -partition 2 -acks 1
```

| 指标 | 结果 |
|------|------|
| 总消息数 | 3000 |
| 预计吞吐量 | ~300 msg/s |

## 混沌工程测试

### 测试用例

| 故障操作 | 观察指标 | 预期结果 | 实际结果 |
|----------|----------|----------|----------|
| `kill -9` Leader 进程 | 查看其他节点日志 | 3秒内选出新 Leader | 单节点模式下无需选主 |
| 暂停 Follower 网络 | ISR 日志打印踢出 | 生产不阻塞，ISR 缩容 | 单节点模式下无 Follower |
| 强制填满磁盘 | 业务日志打印 `DiskFull` | 优雅拒绝写入，不 Panic | 待验证 |

## 性能优化建议

1. **批量写入优化**：在 `bridge.go` 中启用批量写入（攒够 100 条再调用一次 C 的 `commit_log_append`），TPS 预计能翻一倍。

2. **连接复用**：当前 producer 每条消息创建新连接，建议使用连接池。

3. **异步发送**：当前 producer 同步等待响应，可改为异步模式提升吞吐量。

4. **C 存储引擎**：当前使用 Go 模拟存储，切换到 C 存储引擎后性能预计有显著提升。

## 结论

- ✅ 单节点模式下 `acks=1` 和 `acks=-1` 性能相当
- ✅ 消息发送成功率 100%
- ✅ 无重定向循环问题
- ⚠️ 吞吐量受限于单节点和同步模式，生产环境建议多节点部署并启用批量写入