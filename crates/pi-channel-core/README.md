# pi-channel-core

`pi-runtime` 与 `pi-qq` 共用的协议与基础工具库：

- `protocol`：JSON-RPC 方法常量、`Submit` / `Task` / `TaskId` / `RuntimeStatus` 结构体与输入校验。
- `message_client`：封装 Bearer Token 鉴权与 1 MiB 响应上限的 JSON-RPC 客户端。
- `paths` / `secrets`：默认数据目录解析、运行时端口发现、Token 读取与 `0600` 私有文件创建。
- `systemd`：用户级 systemd unit 渲染与安装。
- `id` / `init_logging`：32 位十六进制随机 ID 生成与 JSON 结构化日志初始化。
