# pi-qq

独立的 QQ Bot v2 通道服务。负责 token、WSS 会话、白名单、消息去重、持久化回发；只通过消息 JSON-RPC 调用运行时，不链接 `pi-runtime` 包。

共享的客户端、消息结构、token 文件工具和 systemd 安装逻辑来自 `pi-channel-core`。

从 workspace 根目录运行：

```bash
cargo run -p pi-qq -- status
cargo build -p pi-qq --release --locked
./scripts/test.sh
```

完整说明：[原生 QQ 指南](../../docs/qq-native.md)。
