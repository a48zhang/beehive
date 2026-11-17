# pi-channel-core

两个服务共用的边界层，保持依赖方向：

```text
pi-runtime → pi-channel-core ← pi-qq
```

- `protocol`：wire DTO、方法名、协议版本、输入校验。
- `message_client`：消息 RPC 客户端及限大小 JSON 响应读取。
- `paths` / `secrets`：数据目录、最小端点发现、token 和私有文件。
- `systemd`：服务传入自身策略，公共层渲染和安装 unit。
- `id` / `init_logging`：统一事件 ID 与 JSON 日志初始化。

不放入 QQ 连接、Pi 会话、任务调度或 SQLite 仓储；这些属于对应服务包。扩展其他通道时只需依赖本包的协议与客户端。
