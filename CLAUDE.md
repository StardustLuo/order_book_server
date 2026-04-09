# CLAUDE.md
## 常用命令

```bash
# 构建
cargo build --release

# 检查（含 clippy lint）
cargo clippy --workspace

# 格式化
cargo fmt --all

# 运行测试
cargo test --workspace

# 运行单个测试
cargo test --package server -- <test_name>

# 运行主服务（Docker 模式）
./target/release/orderbook_server --address 0.0.0.0 --port 8000 --data-dir /path/to/data

# 运行主服务（Direct 模式）
./target/release/orderbook_server --snapshot-mode direct --hlnode-binary /path/to/hl-node --data-dir /path/to/data
```

## Lint 配置

workspace 级别启用了非常严格的 clippy lint（pedantic、nursery、cargo 等全开），同时禁止 `unwrap_used` 和 `expect_used`。Rust lint 也启用了大量额外警告（`unsafe_code`、`unused_crate_dependencies` 等）。详见根 `Cargo.toml` 的 `[workspace.lints]`。

rustfmt 配置：`max_width = 120`，`imports_granularity = "Crate"`，`group_imports = "StdExternalCrate"`。

## 项目架构

Hyperliquid 订单簿 WebSocket 服务器——从本地 Hyperliquid 节点的 streaming 文件中读取实时事件，维护内存订单簿，通过 WebSocket 广播给客户端。

### Workspace 结构

- **`server/`** — 核心库 crate，包含所有业务逻辑
- **`bin/`** — 二进制入口：`orderbook_server`（主服务）、`evm_ws_server`（EVM 数据流）、`latency_official_cmp`（延迟对比工具）

### 核心数据流

```
Hyperliquid 节点 → *_streaming/ 目录（NDJSON 文件）
    → 3 个独立 inotify 线程（order diffs / order statuses / fills）
    → crossbeam channel → tokio async bridge
    → OrderBookState（内存订单簿，逐事件更新，不按区块批量）
    → broadcast channel → WebSocket 客户端（带去重）
```

### 关键模块（server/src/）

| 路径 | 职责 |
|------|------|
| `listeners/order_book/parallel.rs` | 3 个并行 inotify 文件监听线程 |
| `listeners/order_book/state.rs` | 订单簿状态机：应用 diff/status，维护 pending 缓存处理乱序到达 |
| `order_book/mod.rs` | 单币种订单簿（链表价格层级 + BTreeMap） |
| `order_book/multi_book.rs` | 多币种订单簿管理，L2/L4 快照生成，BBO 去重 |
| `servers/websocket_server.rs` | 主 WebSocket 服务器（axum + yawc），订阅管理与消息分发 |
| `servers/evm_ws_server.rs` | EVM 区块/回执数据流服务 |
| `types/subscription.rs` | WebSocket 订阅类型定义（bbo/l2Book/l4Book/trades/bookDiffs/orderUpdates） |
| `types/node_data.rs` | 节点事件数据结构 |
| `metrics.rs` | Prometheus 指标（25+ 指标，lazy_static 注册） |

### 性能关键设计

- **逐事件处理**：不按区块批量，每个 diff/status/fill 到达即处理
- **去重**：BBO 按 px/sz 变化去重（~1μs），L2 按快照哈希去重（~10μs）
- **热路径 JSON 解析**：使用 `sonic-rs` 而非 `serde_json`
- **乱序缓存**：OrderStatus 和 OrderDiff 可能乱序到达，state.rs 中有双向 pending 缓存
- **crossbeam → tokio 桥接**：文件监听在阻塞线程中运行，通过 crossbeam channel 桥接到 async 运行时

### WebSocket 订阅类型

`bbo`、`l2Book`（支持 nSigFigs/nLevels/mantissa 参数）、`l4Book`、`trades`、`bookDiffs`、`orderUpdates`（按用户地址过滤）

### CLI 参数

关键参数：`--markets [perps|spot|hip3|all]`、`--compression-level [0-9]`、`--bbo-only`、`--snapshot-mode [docker|direct]`、`--metrics-port`、`--log-level`。详见 README.md。
