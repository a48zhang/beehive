# Beehive

Beehive 是基于 Rust 构建的本地常驻 Pi Agent 运行时与 QQ 通道套件，由两个独立守护进程和一个共享协议库组成：

```text
QQ ↔ pi-qq.service ── JSON-RPC 2.0 ── pi-runtime.service ↔ Pi RPC 子进程
          │                                  │
      QQ 收件箱/回发状态                  任务队列/调度/会话
          └────────── pi-channel-core ────────┘
                 共享协议、客户端与服务工具
```

仓库地址：[a48zhang/beehive](https://github.com/a48zhang/beehive)

## 目录结构

```text
Cargo.toml                       Workspace 配置与统一依赖版本
crates/
  pi-runtime/                    Pi 运行时守护进程与 CLI（pi-runtime）
    src/                         Pi 子进程生命周期、SQLite 队列、心跳、定时调度、HTTP/RPC
    tests/                       基于模拟 RPC peer 的集成测试
  pi-qq/                         QQ Bot v2 通道守护进程与 CLI（pi-qq）
    src/                         QQ 鉴权、WSS 网关、白名单过滤、收件箱与分片回发
    tests/                       端到端模拟网关集成测试
  pi-channel-core/               共享协议与基础工具库
    src/protocol.rs              Submit / Task / TaskId / RuntimeStatus 等协议结构
    src/message_client.rs        带 Bearer 鉴权的 JSON-RPC 客户端
    src/paths.rs                 默认数据目录与本地端点发现
    src/secrets.rs               Token 读取与 0600 权限文件创建
    src/systemd.rs               用户级 systemd unit 模板渲染与安装
scripts/
  check.sh                       执行 rustfmt、Clippy 与全量测试
  test.sh                        编译测试二进制并运行跨进程集成测试
docs/
  runtime.md                     pi-runtime 配置、CLI、HTTP/RPC 接口与调度说明
  qq-native.md                   pi-qq 配置、Hermes 迁移、状态流转与运维说明
```

包间依赖保持单向：`pi-runtime` 与 `pi-qq` 仅依赖 `pi-channel-core`，通过本地 HTTP JSON-RPC 2.0 通信，各自维护独立的配置文件与 SQLite 数据库。

## 构建与测试

环境要求：Linux、Rust 1.88+；运行 `pi-runtime` 需已安装并完成认证的 Pi（1.5.2+）与 Node.js（22.19+）。

```bash
# 编译 Release 二进制（产出 target/release/pi-runtime 与 target/release/pi-qq）
cargo build --workspace --release --locked

# 单独编译或查看状态
cargo build -p pi-runtime --release --locked
cargo build -p pi-qq --release --locked
cargo run -p pi-runtime -- status
cargo run -p pi-qq -- status

# 代码检查与全量测试
./scripts/check.sh
./scripts/test.sh
```

> 注：集成测试通过 `--all-features` 启用 `test-peer` 模拟网关与假 Pi 进程；日常生产编译请直接使用上面的 `cargo build --workspace --release --locked`。

## 部署与升级

两个服务默认以用户级 systemd 单元运行，仅监听本机回环地址：

| 服务 | systemd 单元 | 默认数据目录 | 本机监听端点 |
| :--- | :--- | :--- | :--- |
| Pi 运行时 | `pi-runtime.service` | `~/.local/share/pi-runtime/` | `127.0.0.1:8787`（REST 与 `POST /rpc`） |
| QQ 通道 | `pi-qq.service` | `~/.local/share/pi-qq/` | `127.0.0.1:8788`（健康与状态查询） |

systemd 模板不设置 `NoNewPrivileges`，Agent 子进程保留当前 Unix 用户正常的 sudo 能力。是否需要密码由系统现有 sudo 策略决定；Beehive 不自动修改 sudoers 或授予免密权限。

首次初始化与部署请参考：
- [Pi 运行时指南](docs/runtime.md)
- [QQ 通道接入与运维指南](docs/qq-native.md)

日常查看状态与日志：

```bash
pi-runtime status
pi-qq status
systemctl --user status pi-runtime pi-qq
journalctl --user -u pi-runtime -u pi-qq -f
```

平滑更新二进制（使用临时文件 + `mv` 原子替换，避免覆盖运行中的可执行文件）：

```bash
install -m 755 target/release/pi-runtime ~/.local/bin/pi-runtime.new
mv ~/.local/bin/pi-runtime.new ~/.local/bin/pi-runtime
install -m 755 target/release/pi-qq ~/.local/bin/pi-qq.new
mv ~/.local/bin/pi-qq.new ~/.local/bin/pi-qq
systemctl --user restart pi-runtime pi-qq
```

## 消息 RPC 概览

`pi-runtime` 在 `POST http://127.0.0.1:8787/rpc` 提供 JSON-RPC 2.0 接口（Bearer Token 鉴权，协议版本 `1`）：

- `message.submit`：幂等入队任务并立即返回 `task_id`
- `message.get`：查询任务执行状态与输出
- `message.cancel`：取消尚处于 `queued` 状态的任务
- `runtime.status`：查询运行时就绪状态与支持的方法列表
