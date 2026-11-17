# pi-runtime

一个小型 Rust 常驻 Pi 运行时，借鉴 Hermes 的「消息入口 + 心跳 + 调度」模式，**不是 Hermes 的完整移植**。

另提供独立 Rust 原生 QQ 通道服务 `pi-qq`，通过消息 JSON-RPC 接入，不依赖 Hermes 网关。见 [原生 QQ 服务与消息 RPC](qq-native.md)。

```text
CLI / HTTP → SQLite 持久化队列 → 单 worker → 常驻 pi --mode rpc
                     ↑                         ↕
              心跳 / every / at / cron       独立会话文件
```

## 功能

- 本地 HTTP REST / JSON-RPC 2.0 消息入口和 CLI；提交立即返回任务 ID，结果通过轮询获取。
- `HEARTBEAT.md` 每 30 分钟检查一次；空文件、标题、HTML 注释和 `- [x]` 已完成项不触发模型调用。
- 固定间隔、一次性 RFC3339 时间、带 IANA 时区的 cron。
- SQLite WAL 持久化队列、任务结果、调度状态、会话映射；数据库自动初始化、版本迁移检查。
- 同一 channel 保留上下文；消息 channel、各个定时任务和心跳的会话相互隔离。会话是上下文隔离，**不是权限隔离**。
- 所有工作串行执行；消息优先于定时任务，定时任务优先于心跳；不会打断已经开始的工作。
- Pi 进程健康检测、崩溃后退避重启、任务执行超时、进程组清理、systemd 托管。
- 默认仅监听 `127.0.0.1`，随机 Bearer token，私有数据目录，输入/队列/结果大小限制，请求 ID 和结构化日志。

## 编译与初始化

需要 Linux、Rust 稳定版、已安装并认证的 Pi 1.5.2+（需要其 `agent_settled` RPC 协议）、Node.js 22.19+。

```bash
cargo build --release --bin pi-runtime
mkdir -p ~/.local/bin
install -m 755 target/release/pi-runtime ~/.local/bin/pi-runtime

pi-runtime init \
  --pi "$HOME/.pi/agent/bin/pi" \
  --cwd "$HOME/work/my-project" \
  --provider YOUR_PROVIDER --model YOUR_MODEL

# 前台验证
pi-runtime serve
# 或注册为常驻用户服务（立即启动）
pi-runtime install-service
```

`init` 默认也读取当前进程的 `PI_PROVIDER`、`PI_MODEL`；不提供模型参数则使用 Pi 默认配置。凭据复用 Pi 的认证存储，不复制密钥。若供应商只通过 shell 环境提供凭据，需要通过 `systemctl --user edit pi-runtime` 配置私有 `EnvironmentFile`；不要把密钥写进源代码。

数据目录默认为 `~/.local/share/pi-runtime/`，可用全局参数 `--home /absolute/path` 或 `PI_RUNTIME_HOME` 修改。`config.json` 中包含工作目录、Pi 路径、模型、工具列表、端口、心跳频率和超时；修改配置后重启服务。

```text
config.json       配置（不含 API key）
token             HTTP Bearer token，0600 权限
HEARTBEAT.md      周期检查清单，运行中修改即可生效
runtime.sqlite3   任务、结果、调度与会话映射
sessions/         Pi 原生会话文件
runtime.lock      单实例锁
```

初始化不覆盖现有配置；安装服务也不覆盖已有 unit。

## 消息入口

```bash
pi-runtime send '检查工作目录，简要汇报当前情况，不修改文件'
pi-runtime send '继续刚才的分析' --channel main
pi-runtime send '分析另一个问题' --channel research
pi-runtime send '稍后执行的任务' --no-wait --key unique-request-123
pi-runtime tasks
pi-runtime task TASK_ID
pi-runtime cancel TASK_ID       # 只允许取消还未开始的任务
pi-runtime status
```

`send` 默认等待最多 1800 秒，可用 `--wait-seconds` 调整；等待超时或关闭客户端**不会取消后台任务**。`--key` 提供持久化的幂等提交键；同键同输入返回原任务，同键不同输入报错。不传 key 时，每次提交都是新任务。

HTTP：

```bash
TOKEN="$(tr -d '\n' < "$HOME/.local/share/pi-runtime/token")"
curl -s http://127.0.0.1:8787/v1/messages \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"channel":"main","message":"你好","idempotency_key":"hello-1"}'
# {"task_id":"..."}

curl -s http://127.0.0.1:8787/v1/tasks/TASK_ID \
  -H "Authorization: Bearer $TOKEN"
unset TOKEN
```

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 无鉴权存活检查，不暴露任务 |
| GET | `/ready` | Pi RPC 是否就绪，需要鉴权 |
| GET | `/v1/status` | Pi 状态、当前任务、最近心跳检查 |
| POST | `/rpc` | JSON-RPC 2.0：`message.submit/get/cancel`、`runtime.status` |
| POST | `/v1/messages` | 入队：`message`, 可选 `channel`, `idempotency_key` |
| GET | `/v1/tasks` | 最近 100 个任务及结果 |
| GET / DELETE | `/v1/tasks/{id}` | 查看 / 取消待执行任务 |
| GET / POST | `/v1/jobs` | 列表 / 新建调度 |
| DELETE | `/v1/jobs/{id}` | 删除调度，不影响已入队或运行中的那一次任务 |

除 `/health` 外都需要 Bearer token；不开放浏览器 CORS，带 `Origin` 的请求直接拒绝。鉴权请求全局限制 30 次/秒。消息最长 16000 字节，HTTP body 上限 32 KiB；待处理队列最多 1000 项、最多 100 条调度、256 个历史会话；这些是小型单用户服务的保护限制。

## 定时任务

```bash
# 每小时：首次执行在一小时后
pi-runtime add-job --name hourly-check --every 3600 \
  --message '只读检查指定项目，简要报告风险，不修改文件'

# 每天北京时间 09:00（六字段：秒 分 时 日 月 星期，可加第七字段年份）
pi-runtime add-job --name daily --cron '0 0 9 * * *' --timezone Asia/Shanghai \
  --message '整理工作目录中用户指定的待办，不进行外部发送'

# 一次性任务：替换为未来的时间，必须带时区
pi-runtime add-job --name once --at '2027-01-01T09:00:00+08:00' \
  --message '生成一份简短工作摘要'

pi-runtime jobs
pi-runtime delete-job JOB_ID
```

HTTP 新建格式：

```json
{
  "name": "daily",
  "message": "简要汇报指定项目状态，不修改文件",
  "timing": {
    "kind": "cron",
    "expression": "0 0 9 * * *",
    "timezone": "Asia/Shanghai"
  }
}
```

也支持 `{"kind":"every","seconds":3600}` 或 `{"kind":"at","at":"2027-01-01T01:00:00Z"}`。最小周期 10 秒；这不是额度预算控制，请不要配置无意义的高频 LLM 调用。

## 心跳

编辑 `~/.local/share/pi-runtime/HEARTBEAT.md`：

```markdown
# Heartbeat
- [ ] 检查 /明确的项目路径 中最近的测试报告；只报告失败，不修改文件。
```

下次心跳检查自动入队。心跳正文每次从磁盘重新读取，执行完不会自动勾选条目；只要未完成项仍存在，就会每周期执行。清空文件或将条目标记 `- [x]` 可停止后续模型调用。文件最多 16000 字节。不要只写“主动做有用的事”之类无边界指令。

## 重启语义与限制

- 已入队、未开始的任务：重启后继续。
- 已开始的任务：优雅停机或下次恢复时标为 `interrupted`，**不自动重放**，因为可能已经有文件写入等副作用。
- 运行失败或超时：标为 `failed`，不做外层自动重试；Pi 自己仍可能按其配置重试供应商临时错误。
- 同一定时任务/心跳有 queued 或 running 实例时，合并后续触发，不累计堆积。
- 服务停机错过多个时刻：恢复时最多补一次，然后从当前时间计算下一次；不逐次追赶。
- 系统时钟用于调度；系统休眠期间不能执行。任务最长默认 900 秒；超时会终止 Pi 进程组，下一任务启用新进程并恢复相应会话。
- 停机不承诺完成当前任务：关闭 Pi stdin、等待 5 秒，然后终止 Pi 进程组。请先等当前任务结束再升级。
- 目前是单机单用户单 worker，没有 Telegram/飞书适配器、前端或多租户。QQ 原生适配器作为独立 `pi-qq.service` 接入，详见 `docs/qq-native.md`；核心结果仍保留在 SQLite，通过 CLI/HTTP/RPC 获取。
- 不自动清理历史任务、结果、会话；长期运行需监控磁盘，并在停服后备份数据库及其 WAL 文件。

## 服务管理

```bash
systemctl --user status pi-runtime
systemctl --user restart pi-runtime
systemctl --user stop pi-runtime
journalctl --user -u pi-runtime -f
# 停用
systemctl --user disable --now pi-runtime
```

若希望用户退出登录后仍运行，需要 `loginctl enable-linger "$USER"`（取决于系统策略，可能需要管理员）；重启开机恢复还依赖用户 systemd manager。已有 Hermes 等服务不受影响。

## 安全边界

**Token 等同于向当前用户权限的 Agent 下发任务的能力。** 默认 Agent 可用 `read,bash,edit,write`，Pi 不会逐次工具确认；工作目录不是沙箱，能访问该 Unix 用户可访问的其他路径。systemd 的 `NoNewPrivileges`、不加载扩展和消息鉴权都不能防御模型提示注入。对不可信消息，应使用独立用户/容器、只暴露必要目录，或在配置中缩减工具。

运行时只显式加载 Pi 内置 MCP 与 codemode 扩展，其余扩展、项目批准、prompt templates 和自动上下文文件禁用，保留普通 skills；用户级 `~/.pi/agent/mcp.json` 中启用的 MCP 服务会被加载。MCP 可访问的远程浏览器/服务也属于需要限制和保护的权限；RPC 交互确认请求一律取消，不会自动批准。不要把 HTTP 端口直接暴露公网；远程访问建议 SSH 隧道。会话与结果可能包含敏感内容，分享前先审阅。提示词中的安全提醒不构成权限隔离。

## 开发验证

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./scripts/test.sh
```

集成测试使用 Rust 编写的假 Pi RPC peer，不访问模型、不消耗 API 额度；覆盖提前到达的事件、Unicode 分隔符、handled 命令、模型失败/拒绝/确认取消、上下文隔离、HTTP 鉴权/校验、幂等提交、一次性调度、空/非空心跳、停机恢复、执行超时后的进程恢复和单实例锁。
