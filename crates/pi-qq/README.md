# pi-qq

QQ Bot v2 通道守护进程，负责管理 Access Token、WSS 网关连接（Identify / Resume / 心跳）、OpenID 白名单过滤、收件箱去重与分片回发，通过本地 JSON-RPC 调用 `pi-runtime`。

常用命令（在 workspace 根目录执行）：

```bash
cargo run -p pi-qq -- status
cargo build -p pi-qq --release --locked
./scripts/test.sh
```

详细配置与运维说明见 [docs/qq-native.md](../../docs/qq-native.md)。
