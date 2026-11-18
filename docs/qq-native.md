# pi-qq 通道服务与消息 RPC 指南

`pi-qq` 是独立的 QQ 官方机器人（Bot v2）通道服务，负责接收 QQ 私聊与群 `@` 消息、转发至 `pi-runtime` 执行，并将结果分片回发至 QQ。

```text
腾讯 QQ Bot v2 ← WSS / REST → pi-qq.service
                                  │
                       SQLite 收件箱 / 回发状态
                                  │ JSON-RPC 2.0 + Bearer Token
                                  ▼
                      pi-runtime.service /rpc
```

## 功能特性

- **消息范围**：支持 QQ 官方机器人 C2C 私聊文本与群聊 `@` 文本消息（自动剥离群聊开头的 `<@!...>` 前缀）。
- **连接与鉴权**：自动缓存并刷新 Access Token（遇 401 自动刷新重试），内置 WSS Gateway Identify、Resume、心跳 ACK 检测与断线指数退避重连。
- **精确白名单**：私聊用户必须匹配 `allowed_users`（OpenID）；群聊必须同时匹配 `allowed_groups`（`group_openid`）与发信人的 `allowed_users`（`member_openid`）。
- **独立会话隔离**：按 `AppID + 场景(c2c/group) + 目标会话 + 发信人` 哈希生成独立的 `pi-runtime` channel，群内不同成员各自拥有独立上下文。
- **持久化去重与分片回发**：事件写入 SQLite 收件箱后才推进 WSS Resume 游标；回发文本按单条最大 3,500 UTF-8 字节自动切分（最多 5 片，`msg_seq` 对应 `1..=5`）。

## 1. 编译与初始化

```bash
cargo build --release --locked --bin pi-runtime --bin pi-qq
install -m 755 target/release/pi-qq ~/.local/bin/pi-qq
```

### 方式 A：直接创建新配置

```bash
pi-qq init --runtime-home "$HOME/.local/share/pi-runtime"
# 随后编辑 ~/.local/share/pi-qq/credentials.json 填入 app_id 与 client_secret
# 并编辑 ~/.local/share/pi-qq/config.json 在 allowed_users 中填入允许的 user_openid
```

### 方式 B：从已有 Hermes 目录一次性导入凭据与用户白名单

```bash
pi-qq init --runtime-home "$HOME/.local/share/pi-runtime" \
  --hermes-home "$HOME/.hermes"
```

该命令从 Hermes 的 `.env` 与 `pairing/qqbot-approved.json` 只读导入 `QQ_APP_ID`、`QQ_CLIENT_SECRET` 及已批准的用户 OpenID（群白名单 `allowed_groups` 默认保持为空）。当配置中保留 `hermes_home` 时，`pi-qq` 会在启动和重连前检查 `gateway_state.json`，防止与正在运行的 Hermes 抢占同一 AppID 的网关连接。

### 数据目录与配置项

默认数据目录为 `~/.local/share/pi-qq/`（可通过 `--home` 或 `PI_QQ_HOME` 修改）：

```text
config.json        运行配置与白名单列表
credentials.json   {"app_id":"...","client_secret":"..."}（0600 权限）
token              本服务状态接口的 Bearer Token（0600 权限）
qq.sqlite3         收件箱、关联 task_id、分片发送进度与 WSS Resume 游标
service.lock       数据目录单实例锁
```

`config.json` 字段说明：

| 字段 | 说明 |
| :--- | :--- |
| `runtime_rpc_url` | 默认 `http://127.0.0.1:8787/rpc`（仅支持本机回环地址） |
| `runtime_token_file` | `pi-runtime` 的 Token 文件绝对路径 |
| `credentials_file` | QQ 机器人凭据文件绝对路径 |
| `health_port` | 本机健康与状态接口端口，默认 `8788` |
| `allowed_users` | 允许访问的精确 `user_openid` / `member_openid` 列表（空列表拒绝所有请求） |
| `allowed_groups` | 允许访问的精确 `group_openid` 列表（空列表禁用群聊响应） |
| `poll_interval_ms` | 轮询收件箱与任务状态的间隔（毫秒），默认 `1000`（范围 `500..=30000`） |
| `hermes_home` | 可选的 Hermes 目录路径，用于检测同 AppID 进程冲突 |

## 2. 注册服务与日常运维

```bash
# 注册 systemd 用户单元（加 --start 可在通过冲突检查后立即启用并启动）
pi-qq install-service --start

# 配置与连通性自检
pi-qq check

# 查看运行状态与最近投递记录
pi-qq status
pi-qq deliveries
journalctl --user -u pi-qq -f
```

若从 Hermes 切换至 `pi-qq`，请先在 Hermes 中停用 QQ 通道（或停止 `hermes-gateway.service`），运行 `pi-qq check` 确认无冲突后再启动 `pi-qq`。

本机状态接口监听 `127.0.0.1:8788`（`/ready` 与 `/v1/status` 需携带 `~/.local/share/pi-qq/token`）：
- `GET /health`：存活检查
- `GET /ready`：当 QQ WSS 网关已就绪且 `pi-runtime` 可用时返回 `200`，否则返回 `503`
- `GET /v1/status`：返回连接状态、重连计数与各阶段投递统计

## 3. 投递状态流转与手动重发

```text
pending → submitted → reply → sending → delivered
                               └→ uncertain（发送中途断连/5xx/进程退出）
```

- `pending` 阶段以事件唯一哈希作为幂等键调用 `message.submit`；进入 `submitted` 后持续轮询 `message.get`，不因消息年龄或重试次数丢弃结果。
- 不再有回复 TTL 或 `dead` 终态。RPC 故障、明确的 QQ 拒绝等保留原阶段，持续退避重试（最长间隔 60 秒），错误可通过 `pi-qq deliveries` 查看。
- 旧配置中的 `reply_ttl_seconds` 会被忽略；启动时会恢复旧 `dead` 记录，保留已有 task_id、幂等键和已确认分片游标，不重新执行 Pi 任务。
- 本地不再限制回复时限，但腾讯服务端仍可能拒绝旧 msg_id 或限制发送额度；这种拒绝会保留为待回发并记录错误，不代表本地能绕过平台限制。
- 每次调用 QQ 发送接口前先将状态持久化为 `sending`，收到腾讯返回的成功消息 ID 后才累加已发送分片数 `sent`。
- 若发送请求超时、返回 `5xx` 或服务在发送途中退出，状态会标记为 `uncertain` 并暂停自动重发；确认需要补发时可执行：

```bash
pi-qq retry <EVENT_KEY> --allow-duplicate
```

## 4. Runtime JSON-RPC 2.0 协议参考

`POST http://127.0.0.1:8787/rpc` 与 REST 接口共用同一套鉴权、队列和幂等表：

```bash
TOKEN="$(tr -d '\n' < "$HOME/.local/share/pi-runtime/token")"
curl -s http://127.0.0.1:8787/rpc \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"req-1","method":"message.submit","params":{"channel":"example","message":"你好","idempotency_key":"event-1"}}'
unset TOKEN
```

| 方法 (`method`) | 参数 (`params`) | 返回结果 (`result`) |
| :--- | :--- | :--- |
| `message.submit` | `message`，可选 `channel`（默认 `"main"`）、`idempotency_key` | `{"task_id": "<32位十六进制ID>"}` |
| `message.get` | `{"task_id": "..."}` | 完整 `Task` 对象（含 `status` / `result` / `error`） |
| `message.cancel` | `{"task_id": "..."}` | `{"cancelled": true}`（仅限 `queued` 状态任务） |
| `runtime.status` | `{}` 或省略 | `{"protocol_version": 1, "status": {...}, "methods": [...]}` |

错误码遵循 JSON-RPC 2.0 规范：`-32700`（JSON 解析失败）、`-32600`（无效请求）、`-32601`（未知方法）、`-32602`（参数校验失败）、`-32603`（内部存储错误）、`-32000`（队列已满或幂等键冲突）、`-32004`（任务不存在）、`-32009`（任务非 `queued` 状态不可取消）。
