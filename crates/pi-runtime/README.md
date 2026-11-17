# pi-runtime

独立的 Pi Agent 运行时服务。负责子进程生命周期、任务队列、会话、心跳、调度以及 REST/JSON-RPC 服务端；不包含 QQ 实现。

依赖 `pi-channel-core` 提供消息 DTO、状态结构、私有文件与 systemd 工具。数据库与 Pi RPC 协议实现属于本包，不暴露给通道服务复用。

从 workspace 根目录运行：

```bash
cargo run -p pi-runtime -- status
cargo build -p pi-runtime --release --locked
```

完整说明：[运行时指南](../../docs/runtime.md)。
