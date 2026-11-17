# 原生 QQ 服务与消息 RPC

`pi-qq` 是独立的 Rust 可执行文件和 systemd 服务，不导入 Hermes Python 模块，不依赖 Hermes 网关进程。

```text
腾讯 QQ Bot v2 ← WSS / REST → pi-qq.service
                                  │
                       SQLite 收件箱 / 回发状态
                                  │ JSON-RPC 2.0 + Bearer token
                                  ▼
                      pi-runtime.service /rpc
                                  │
                          SQLite 任务队列
                                  │
                           常驻 Pi RPC 子进程
```

## 支持范围

- QQ 官方机器人 **C2C 私聊文本**、**群 @ 文本**；不是个人 QQ 登录或 OneBot/NapCat。
- QQ token 缓存/自动刷新，401 后刷新再试，Gateway Identify、Resume、心跳 ACK 检测、断线退避重连。
- 精确 openid 白名单：私聊必须匹配 `allowed_users`；群消息必须同时匹配 `allowed_groups` 与发信人的 `allowed_users`。
- 每个 AppID / 场景 / 会话 / 发信人独立 Pi 上下文；不会把整个群的不同成员混到一个工具会话。
- QQ 消息落盘后才推进持久化 Resume 游标；重复事件不重复执行模型任务。
- 回发使用原始 `msg_id`，`msg_seq` 按分片固定为 1..5；纯文本、每片最多 3500 UTF-8 字节、最多 5 片，超出部分提示截断。
- 健康检查、私有状态接口、单实例/同 AppID 锁、SIGTERM 优雅停止、systemd 托管。

**首版不支持**图片/文件传入 Pi、语音转写、QQ 频道/guild、按钮审批、流式回复、主动推送、QQ 内 `/new`/`/stop` 命令。带文本的多媒体消息只取其文本；纯多媒体消息忽略。现有 Hermes 历史不自动导入。

## 1. 编译

```bash
cargo build --release --locked --bin pi-runtime --bin pi-qq
install -m 755 target/release/pi-qq ~/.local/bin/pi-qq
# 更新 runtime 二进制后，等当前任务结束再重启 pi-runtime。
```

生产构建不要开启 `test-peer` feature。测试专用的模拟 QQ HTTP 地址参数只存在于此 feature，普通 release 没有该参数。

## 2. 初始化

复用现有 Hermes Bot 身份：

```bash
pi-qq init --runtime-home "$HOME/.local/share/pi-runtime" \
  --hermes-home "$HOME/.hermes"
```

这只是**一次性只读导入**：从 Hermes `.env` 读取 `QQ_APP_ID` / `QQ_CLIENT_SECRET`，从 `QQ_ALLOWED_USERS` 和 `pairing/qqbot-approved.json` 取已允许的用户。密钥写入新服务自己的 `credentials.json`，不打印密钥、不修改 Hermes、不联网、不建立第二条 QQ 连接。群白名单默认空，**即便 Hermes 曾开放群消息也不会自动开放**。

没有 Hermes 时：

```bash
pi-qq init --runtime-home "$HOME/.local/share/pi-runtime"
# 手工填写 credentials.json，以及 config.json 的 allowed_users
```

数据目录默认为 `~/.local/share/pi-qq/`，可用 `--home` / `PI_QQ_HOME` 修改：

```text
config.json        运行配置与白名单
credentials.json   {"app_id":"...","client_secret":"..."}，0600
token              本服务状态接口的 token（与 runtime token 不同），0600
qq.sqlite3         收件箱、任务关联、分片进度、Resume 状态
service.lock       数据目录互斥锁
```

目录权限 0700。`runtime_token_file` 指向 runtime 的私有 token 文件，而不是把 token 写进配置。

配置字段：

| 字段 | 说明 |
|---|---|
| `runtime_rpc_url` | 默认 `http://127.0.0.1:8787/rpc`，只允许本机 HTTP，不跟随重定向/代理 |
| `runtime_token_file` | Runtime Bearer token 文件绝对路径 |
| `credentials_file` | QQ 凭据文件绝对路径 |
| `health_port` | 本机健康/状态接口，默认 8788 |
| `allowed_users` | 精确 QQ openid 列表，空列表拒绝全部用户，不支持 `*` |
| `allowed_groups` | 精确 group_openid 列表，空列表禁用群接入 |
| `poll_interval_ms` | 默认 1000，批量轮询最多 10 条待处理事件 |
| `reply_ttl_seconds` | 默认 240，允许 60..3600，超过后不再尝试旧消息回复 |
| `hermes_home` | 导入源，用于启动/重连前检查是否与 Hermes 抢占；非导入场景可为 null |

注意：普通 QQ 号码不是 openid。群内 `member_openid` 和 C2C `user_openid` 可能不同；要放行群成员，需要配置该场景的真实 openid，不能猜测或直接复用 QQ 号。

修改配置/白名单/凭据或轮换 token 后重启 `pi-qq`。目前不热加载。

## 3. 注册服务与切换

```bash
pi-qq install-service
systemd-analyze --user verify ~/.config/systemd/user/pi-qq.service
```

默认只注册 unit，**不启动、不启用自启**。切换前确认：

1. 让 Hermes 的 QQ 当前任务结束。
2. 通过 Hermes 配置禁用其 QQ 入口并重启 Hermes；其他通道可以保留。若确定整个 Hermes 网关都不再使用，也可以停用整个 `hermes-gateway.service`。
3. 运行 `pi-qq check`，确认没有连接占用，且消息 RPC 可达。
4. `systemctl --user enable --now pi-qq`。
5. 在白名单 QQ 私聊发送一条无副作用的测试文本，确认只收到一份回复。

**不要只删 Hermes 的锁文件，也不要让同 AppID 两个客户端并行抢连接。** 从 Hermes 导入时，原生服务会检查其运行状态，发现同 AppID 的活动 QQ 入口就拒绝启动。此保护不是腾讯端的全局租约：仍需由操作者确保其他机器或未登记的客户端不同时连接。`hermes_home` 不应为了绕过占用检查而清空。

若停用整个 Hermes 服务，需要同时处理其自启，避免下次重启又连回来。`pi-qq` 还会在每次重连前重新检查 Hermes 占用。

可选 `pi-qq install-service --start` 会先做占用检查，再注册并启用服务；已有 unit 不覆盖。

回滚：先 `systemctl --user disable --now pi-qq`，再恢复 Hermes QQ 配置/服务，不能反过来先同时启动。

## 4. 运维

```bash
pi-qq status
pi-qq deliveries
pi-qq check
journalctl --user -u pi-qq -f
systemctl --user restart pi-qq
```

端点只监听 `127.0.0.1:8788`：

- `GET /health`：公开存活检查。
- `GET /ready`：QQ Gateway Ready 且 runtime Pi 可用时返回 200，否则 503。
- `GET /v1/status`：QQ/runtime 状态、重连次数、各回发状态计数。

除 `/health` 外都需要 `~/.local/share/pi-qq/token` 中的 Bearer token，不开放浏览器 Origin。

### 持久化与发送语义

```text
pending → submitted → reply → sending → delivered
                               ├→ uncertain
                               └→ dead（明确拒绝/过期/重试耗尽）
```

- `pending` 的 RPC 提交使用稳定事件幂等键。即使提交后进程退出、没拿到 task_id，重试也会拿回原任务，不重复执行模型。
- `submitted` 持久化 task_id，重启后继续轮询。不主动重跑 failed/interrupted 的 Pi 任务。
- 每次外部发送前落盘为 `sending`；只有 QQ 返回成功消息 ID 才推进分片游标。
- 401 是明确未授权，刷新 token 后再试；429 / 尚未建立连接等明确未发送故障退避重试，最多 5 次。
- 请求超时、连接中途断开、5xx、成功应答无法确认、进程在发送中退出，都可能已经产生副作用，标为 `uncertain`，**不自动重发**。稳定 msg_seq 有助于腾讯去重，但不宣称端到端 exactly-once。
- 若已确认可以重发，可显式 `pi-qq retry EVENT_KEY --allow-duplicate`，从未确认的分片继续；可能重复发送。不会新建已关联过的 Pi 任务。
- QQ 不保证离线事件永久排队，Resume 也受腾讯服务端限制；本地持久化不能恢复服务端未投递的消息。
- 默认 TTL 是保守的回复时限，不保证等于 QQ 平台实际额度/有效期。排队或模型执行超过 TTL 后标为 dead，**已接受的 Pi 任务不会因此被取消**，可用 `pi-runtime task TASK_ID` 取结果；不退化成未经授权的主动推送。
- 收件箱最多 1000 个活动事件；历史/结果不自动清理，请监控磁盘并在停服后备份。
- `pi-qq` 的 HTTP 请求支持 reqwest 标准代理环境；WebSocket 当前需要可直连的 WSS 网络，不实现 WSS 代理。

## 5. Runtime 消息 RPC

`POST http://127.0.0.1:8787/rpc`，JSON-RPC **2.0 单请求**，与 `/v1/*` REST 共用鉴权、队列、校验和幂等记录。它不是 Pi 子进程的 stdin/stdout RPC，也不向通道开放 `bash`/任意 Pi RPC 命令。

```bash
TOKEN="$(tr -d '\n' < "$HOME/.local/share/pi-runtime/token")"
curl -s http://127.0.0.1:8787/rpc \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"req-1","method":"message.submit","params":{"channel":"example","message":"只回复 hello，不调用工具","idempotency_key":"example-event-1"}}'
unset TOKEN
```

成功：

```json
{"jsonrpc":"2.0","id":"req-1","result":{"task_id":"32位十六进制任务ID"}}
```

| method | params | result |
|---|---|---|
| `message.submit` | `message`, 可选 `channel`（默认 main）、`idempotency_key` | `task_id`，只表示已接受，不是执行完成 |
| `message.get` | `task_id` | 任务，含 `status/result/error` |
| `message.cancel` | `task_id` | `cancelled:true`，仅 queued 任务 |
| `runtime.status` | `{}` 或省略 | `protocol_version:1`、状态、方法列表 |

普通接口错误用 JSON-RPC `error`：`-32700` 无效 JSON、`-32600` 无效请求/批请求、`-32601` 未知方法、`-32602` 参数错误、`-32603` 内部错误、`-32000` 提交拒绝（队列满/幂等冲突）、`-32004` 任务不存在、`-32009` 任务不可取消。HTTP 鉴权/限流/body 上限仍用 401/429/413 等状态码。

支持 notification（无 id），执行后返回 HTTP 204，不返回错误或结果；通道投递必须使用带 id 的请求以及幂等键，以便确认入队。暂不支持 batch、SSE 或服务端回调。

## 安全边界

白名单内 QQ 发信人可向 Pi 下达工具任务；这等同于授予本机 Unix 用户权限的 Agent 入口。默认 `read,bash,edit,write` 并非沙箱，授权用户或提示注入可能影响用户目录乃至凭据。不要开放给不可信群成员，生产建议独立 Unix 用户/容器或进一步限制 Pi 工具。

源码、日志和服务命令行不含 QQ Secret。运行状态只公开计数/哈希事件键，不打印聊天正文、收件人 openid 或 access token；私有 SQLite 与会话仍可能含敏感正文，需要保护和审阅。

## 测试

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./scripts/test.sh
```

模拟 QQ HTTP/WSS 与假 Pi peer 覆盖：JSON-RPC/REST 幂等共享、协议错误、鉴权、通知、QQ Identify/Resume、心跳 ACK、重复事件、陌生用户/群拒绝、401 token 刷新、文本回发、5xx 不确定发送，以及重启后不重复模型调用/回发。测试不连接腾讯、不发送真实 QQ 消息、不调用真实模型。
