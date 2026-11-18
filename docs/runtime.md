# pi-runtime 运行时指南

`pi-runtime` 是一个常驻本地的 Pi Agent 守护进程，提供消息队列、周期性心跳检查与定时任务调度。

```text
CLI / HTTP / RPC → SQLite 持久化队列 → 单 worker → 常驻 pi --mode rpc
                          ↑                          ↕
                   心跳 / every / at / cron       按 channel 隔离会话文件
```

## 核心机制

- **异步入队与轮询**：CLI 与 HTTP/JSON-RPC 提交消息后立即返回 `task_id`，由后台单 worker 串行消费。
- **优先级队列**：消息（`message`）优先于定时任务（`job:*`），定时任务优先于心跳（`heartbeat`）；已开始的任务不会被抢占。
- **多通道会话保持**：同一 `channel` 自动复用对应的 Pi 会话文件；不同 `channel`、各定时任务与心跳拥有各自独立的上下文（保留最近使用的 256 个会话映射，删除定时任务时自动清理对应映射）。
- **子进程自愈**：自动对 `pi --mode rpc` 进行健康检查；发生进程 I/O 异常或任务执行超时（默认 900 秒）时自动清理进程组并退避重启。
- **周期心跳**：默认每 30 分钟检查一次 `HEARTBEAT.md`；当存在未勾选的待办项（如 `- [ ] ...`）时触发执行，空文件、纯注释或全部已完成（`- [x]`）时不调用模型。

## 编译与初始化

需要 Linux、Rust 1.88+、已安装并登录认证的 Pi 1.5.2+ 及 Node.js 22.19+。

```bash
cargo build --release --locked --bin pi-runtime
mkdir -p ~/.local/bin
install -m 755 target/release/pi-runtime ~/.local/bin/pi-runtime

pi-runtime init \
  --pi "$HOME/.pi/agent/bin/pi" \
  --cwd "$HOME/work/my-project" \
  --provider YOUR_PROVIDER --model YOUR_MODEL

# 前台运行验证
pi-runtime serve
# 或注册并启动用户级 systemd 服务
pi-runtime install-service
```

`init` 也支持从环境变量 `PI_PROVIDER`、`PI_MODEL` 读取默认模型参数；若模型依赖环境变量传入 API Key，可通过 `systemctl --user edit pi-runtime` 配置 `EnvironmentFile`。

默认数据目录为 `~/.local/share/pi-runtime/`（可通过 `--home` 或 `PI_RUNTIME_HOME` 指定）：

```text
config.json       运行配置（pi 路径、cwd、端口、工具列表、超时与心跳间隔）
token             HTTP Bearer Token（0600 权限）
HEARTBEAT.md      周期检查清单（运行中修改即在下个周期生效）
runtime.sqlite3   任务队列、执行结果、定时任务与会话映射
sessions/         Pi 会话历史文件目录
runtime.lock      单实例文件锁
```

## CLI 与 HTTP 接口

### CLI 常用命令

```bash
pi-runtime send '检查当前仓库状态并汇总近期改动'
pi-runtime send '继续刚才的分析' --channel main
pi-runtime send '后台长任务' --no-wait --key unique-request-123
pi-runtime tasks
pi-runtime task TASK_ID
pi-runtime cancel TASK_ID       # 仅可取消尚处于 queued 状态的任务
pi-runtime status
```

`send` 默认阻塞等待结果（最长 1800 秒，可通过 `--wait-seconds` 调整）；客户端超时退出不会中断后台任务。传入 `--key` 可实现持久化幂等提交。

### HTTP 端点

默认监听 `127.0.0.1:8787`，除 `/health` 外均需携带 `Authorization: Bearer <TOKEN>` 请求头（拒绝携带浏览器 `Origin` 头的请求）：

| 方法 | 路径 | 说明 |
| :--- | :--- | :--- |
| GET | `/health` | 存活检查（免鉴权） |
| GET | `/ready` | 检查常驻 Pi 子进程是否就绪 |
| GET | `/v1/status` | 返回 Pi 就绪状态、当前执行任务 ID 与最近心跳时间戳 |
| POST | `/rpc` | JSON-RPC 2.0 入口（`message.submit` / `get` / `cancel`、`runtime.status`） |
| POST | `/v1/messages` | 提交消息：`{"message":"...", "channel":"main", "idempotency_key":"..."}` |
| GET | `/v1/tasks` | 查询最近 100 条任务记录 |
| GET / DELETE | `/v1/tasks/{id}` | 查询单条任务详情 / 取消 `queued` 状态任务 |
| GET / POST | `/v1/jobs` | 列出 / 新建定时任务 |
| DELETE | `/v1/jobs/{id}` | 删除定时任务并清理其关联会话 |

请求与队列配额：单条消息最大 16,000 字节，HTTP 请求体上限 32 KiB，结果落盘上限 256 KiB，待处理队列上限 1,000 条，定时任务上限 100 个。

## 定时任务与心跳

### 定时任务（CLI / HTTP）

```bash
# 固定间隔（每 3600 秒执行一次，最小间隔 10 秒）
pi-runtime add-job --name hourly-check --every 3600 \
  --message '检查测试报告并汇总失败项'

# Cron 表达式（6 字段：秒 分 时 日 月 星期，支持可选第 7 字段年份）
pi-runtime add-job --name daily --cron '0 0 9 * * *' --timezone Asia/Shanghai \
  --message '生成今日待办摘要'

# 一次性任务（RFC3339 时间戳）
pi-runtime add-job --name once --at '2027-01-01T09:00:00+08:00' \
  --message '生成阶段工作汇总'

pi-runtime jobs
pi-runtime delete-job JOB_ID
```

通过 `POST /v1/jobs` 创建时的 JSON 结构：

```json
{
  "name": "daily",
  "message": "生成今日待办摘要",
  "timing": {
    "kind": "cron",
    "expression": "0 0 9 * * *",
    "timezone": "Asia/Shanghai"
  }
}
```

`timing` 同样支持 `{"kind": "every", "seconds": 3600}` 与 `{"kind": "at", "at": "2027-01-01T01:00:00Z"}`。当同一任务仍有 `queued` 或 `running` 实例时，后续触发会自动合并；停机期间错过的多次触发在恢复时最多补执行一次。

### 心跳清单

编辑 `~/.local/share/pi-runtime/HEARTBEAT.md`：

```markdown
# Heartbeat
- [ ] 检查指定项目目录中的最新构建日志并汇报异常
```

每次心跳到期时从磁盘重新读取该文件（上限 16,000 字节）。将条目改为 `- [x]` 或清空文件即可停止周期调用。

## 重启恢复与服务管理

- **任务状态恢复**：服务重启后，`queued` 任务继续排队执行；重启前正处于 `running` 状态的任务会被标记为 `interrupted`（不自动重跑，避免重复产生副作用）。
- **权限说明**：`pi-runtime` 默认向 Pi 启用 `read,bash,edit,write,codemode` 工具且运行在无人值守模式（自动拒绝交互式弹窗确认），拥有当前 Unix 用户的本地文件权限，请勿将 HTTP 端口直接暴露到公网。

常用 systemd 管理命令：

```bash
systemctl --user status pi-runtime
systemctl --user restart pi-runtime
systemctl --user stop pi-runtime
journalctl --user -u pi-runtime -f
# 开启用户退出登录后继续常驻运行
loginctl enable-linger "$USER"
```
