# Beehive

一个 Rust workspace，包含 **两个独立服务包** 和一个共享库：

```text
QQ ↔ pi-qq.service ── JSON-RPC 2.0 ── pi-runtime.service ↔ Pi RPC 子进程
          │                                  │
      QQ 收件箱/回发状态                  任务队列/调度/会话
          └────────── pi-channel-core ────────┘
                 共享协议、客户端与服务工具
```

项目由原 `pi-runtime` 单包重构而来，两个服务可独立构建、部署。仓库：[a48zhang/beehive](https://github.com/a48zhang/beehive)。

## 目录与边界

```text
Cargo.toml                       workspace、统一版本和依赖
Cargo.lock                       两个服务共用锁文件
crates/
  pi-runtime/                    包/可执行文件：pi-runtime
    src/                         Pi 生命周期、SQLite 队列、心跳、定时、HTTP/RPC
    tests/                       RPC peer 与运行时恢复测试
  pi-qq/                         包/可执行文件：pi-qq
    src/                         QQ 鉴权、WSS、白名单、收件箱、回发
    tests/                       模拟 QQ → 消息 RPC → Pi 的黑盒集成测试
  pi-channel-core/               共享库，无可执行文件
    src/protocol.rs              Submit / Task / TaskId / RuntimeStatus 等协议结构
    src/message_client.rs        带鉴权的消息 RPC 客户端、受限响应读取
    src/paths.rs                 默认数据目录、runtime 端点发现
    src/secrets.rs               token 读取、0600 文件创建
    src/systemd.rs               统一用户服务 unit 模板及安装
scripts/
  check.sh                       格式、Clippy、构建和全量测试
  test.sh                        构建跨包测试进程后运行全量测试
docs/
  runtime.md                     Pi 运行时、REST、心跳和定时任务
  qq-native.md                   QQ 服务、消息 RPC、安全和切换说明
```

依赖方向严格为：

```text
pi-runtime → pi-channel-core ← pi-qq
```

两个服务不互相链接 Rust 包；QQ 通过共享 DTO 和 HTTP JSON-RPC 与 runtime 通信，不读取 runtime 数据库，也不导入其业务配置。共享库不包含 Pi 生命周期、QQ 协议、调度器或数据库实现。

## 构建与验证

需要 Linux、Rust 1.88+；runtime 另需已安装、已认证的 Pi / Node.js。

```bash
cd /home/vlad/work/pi-services

# 生产构建：两个服务一起构建，不包含模拟服务器
cargo build --workspace --release --locked

# 单独构建或运行一个服务
cargo build -p pi-runtime --release --locked
cargo build -p pi-qq --release --locked
cargo run -p pi-runtime -- status
cargo run -p pi-qq -- status

# 推荐的完整检查
./scripts/check.sh
# 只运行构建与测试
./scripts/test.sh
```

脚本优先使用 `CARGO` / PATH 中的 cargo，也兼容 `$HOME/.cargo/bin/cargo`。QQ 集成测试是黑盒跨进程测试，需要同时构建 runtime、QQ 与测试 peer，因此使用脚本，而不依赖另一个服务包的 Rust 实现。测试不连接真实 QQ、不访问浏览器、不调用模型。

产物仍为 `target/release/pi-runtime`、`target/release/pi-qq`。禁止使用 `--all-features` 生产构建：`test-peer` 仅用于模拟网关测试。

## 运行与已有部署

此次整理保持以下接口与路径不变：

| 服务 | systemd unit | 数据目录 | 本机接口 |
|---|---|---|---|
| Pi 运行时 | `pi-runtime.service` | `~/.local/share/pi-runtime/` | `127.0.0.1:8787`，`POST /rpc` |
| QQ 通道 | `pi-qq.service` | `~/.local/share/pi-qq/` | `127.0.0.1:8788`，健康/状态 |

已部署的 systemd unit 仍从 `~/.local/bin/` 启动程序，与源码目录无关。配置、token、QQ Secret、数据库、会话、MCP 配置均不搬入仓库，不需要重新初始化或重新配对 QQ。

```bash
pi-runtime status
pi-qq status
systemctl --user status pi-runtime pi-qq
journalctl --user -u pi-runtime -u pi-qq -f
```

升级时先构建；等待 runtime 没有执行任务、QQ 没有正在回发的消息，再安装新二进制并重启。使用临时文件 + `mv` 替换，避免直接覆盖运行中的可执行文件：

```bash
install -m 755 target/release/pi-runtime ~/.local/bin/pi-runtime.new
mv ~/.local/bin/pi-runtime.new ~/.local/bin/pi-runtime
install -m 755 target/release/pi-qq ~/.local/bin/pi-qq.new
mv ~/.local/bin/pi-qq.new ~/.local/bin/pi-qq
systemctl --user restart pi-runtime pi-qq
```

升级前正在执行的任务不保证完成；中断的 Pi 任务不自动重放，发送中的 QQ 消息可能标为 `uncertain`。不要为了更新源码而重建数据目录。

新部署请分别参考 [运行时指南](docs/runtime.md)、[QQ 接入和切换指南](docs/qq-native.md)。当前使用用户级 systemd、`Restart=always`、5 秒重试；退出登录后继续运行依赖 `Linger=yes`。QQ 配置/权限类 fatal 错误（退出码 78）不会无限自动重启。

## 消息 RPC 契约

`POST /rpc` 使用 Bearer token 鉴权，仅监听本机；协议版本仍为 **1**，JSON-RPC 为 **2.0**：

- `message.submit`：持久化、幂等入队，立即返回 task_id。
- `message.get`：查询状态和结果。
- `message.cancel`：只取消 queued 任务。
- `runtime.status`：能力与就绪状态。

Wire DTO 和方法名以 `pi-channel-core::protocol` 为单一来源；各服务保留自己的持久化模型。版本 0.3 的 workspace 重构不改变已有 JSON、配置或数据库格式。

## 安全边界

QQ 白名单用户可以向本机 Agent 下达工具任务；工作目录不是沙箱。保护 token、QQ 凭据、浏览器会话与私有数据库，不对公网开放接口，不把私有数据复制到工作目录。浏览器 MCP 仍由 Pi 的 `~/.pi/agent/mcp.json` 配置，与 Rust workspace 解耦。
