# pi-runtime

常驻 Pi Agent 运行时守护进程，负责管理 `pi --mode rpc` 子进程生命周期、SQLite 持久化任务队列、多通道会话映射、周期性心跳与定时任务，并提供本地 CLI、REST 与 JSON-RPC 2.0 接口。

常用命令（在 workspace 根目录执行）：

```bash
cargo run -p pi-runtime -- status
cargo build -p pi-runtime --release --locked
```

详细配置与接口说明见 [docs/runtime.md](../../docs/runtime.md)。
