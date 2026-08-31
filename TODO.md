# TODO

## Async IO 优化

> **结论（2026-08-27 定稿）**：所有可测场景下 async 均未胜过 sync——写持平、
> 大读 -4%~-7%、cached 小读 -32%~-46%（跨介质、跨线程数、跨客户端并发度复现）。
> 唯一的理论赢面（cache miss 真盘 + 高并发在途）**无法在本环境验证**：
> oxs-dev 的 ali5000 内核页缓存驱逐被厂商定制禁用（posix_fadvise(DONTNEED)
> 返回 0 但 mincore 显示 100% 驻留，ext4 和 tmpfs 均如此；无 sudo 无法
> drop_caches），真实冷读不可构造。且 FUSE 协议本身无 fadvise opcode（内核
> v6.6 之前均无），fio --invalidate=1 经 fuse 恒为静默 no-op，daemon 侧无法补救。
> 最后一次 1:1 服务 × 8 客户端并发测试（本想测冷读，实际仍是 cache 命中，
> 证据：sync 82K IOPS 超过裸盘 QD8 上限 36K）：seqread -3.6%、randread-4k -32.1%。
>
> **分支定位调整**：原生异步 READ/WRITE 在 5.10 fusedev 上不提供正收益；分支价值
> 转为 multi-task transport 与 FUSE_URING（内核 ≥6.14）的前置基建（runtime 分发、
> borrowed fd、pipelined dispatch）。不再投入 cached 场景优化（RWF_NOWAIT 混合路径
> 收益上限仅为追平 sync，暂挂起）。

### ~~【最高优先级】buffered cache miss 场景验证~~（不可行，已关闭）

**关闭原因**：oxs-dev 无法构造冷缓存（见顶部结论）。多次尝试均被页缓存污染：
1. fio --invalidate=1 → FUSE 协议无 fadvise opcode，静默丢弃；
2. 直接对 backing 文件 fadvise(DONTNEED) → ali5000 内核驱逐失效（返回 0 不生效）；
3. drop_caches → 无 sudo。

若未来能在驱逐正常的宿主上测试，再重启此项；按预承诺判决规则，当前证据已足以定性。

### 【暂挂起】cached 读混合路径：inline RWF_NOWAIT 优先，EAGAIN 回落 io_uring

针对"固定+每字节"双重开销：async_read 先用 RWF_NOWAIT preadv 内联尝试，
命中 cache 时零 io_uring 开销，未命中时回落 io_uring。暂挂起原因：收益上限仅为
追平 sync（无正收益），优先级让位于 multi-task transport。若未来目标内核为 6.13+
（tmpfs NOWAIT 支持），可重启评估。

### Multi-task transport（N 任务 × N ring）

**依据**：真实磁盘 DIO 对比（oxs-dev SATA SSD，--direct=1）中 async 单任务全面
落后 sync 4 线程（顺序 0.72x、随机 4k 读 0.26x）；async 的 DIO 走同步中继，
本质是 1 线程对 4 线程；客户端并发 4→16 无改善，瓶颈在单服务线程。per-thread
吞吐 async 并不差（顺序读单线程 213MB/s vs sync 单线程 ~75MB/s），缺的是并行
服务单元。另：256K 探测显示 async 读聚合带宽封顶 ~1.4-1.7 GB/s，多任务可解除该封顶。

**方案**：多个 FuseDevTask 各自运行单线程 runtime + io_uring ring 并发读
/dev/fuse（对齐现有 sync 模式 N-channel 模型，也是 FUSE_URING per-CPU ring 的
基础）。

### READDIR/READDIRPLUS 卸载到 blocking pool

**问题**：async 模式下 READDIR/READDIRPLUS 没有异步实现，server 分发直接调用同步
handler（`src/api/server/async_io.rs` 中 `self.readdir(ctx)` 无 `.await`），在单线程
uring runtime 线程上内联执行 `lseek64` + `getdents64` 循环，并在整个过程中持有
`HandleData.lock`。读取大目录时阻塞 runtime 线程，导致所有并发请求和 io_uring
完成事件停摆（head-of-line blocking）。

**方案**：给 `AsyncFileSystem` trait 增加 `async_readdir`/`async_readdirplus`，
PassthroughFs 中通过 `spawn_blocking` 中继到同步 handler（与其他 relay 类操作相同
模式）。io_uring 没有 GETDENTS opcode，offload 是最终方案而非过渡方案。

**注意点**：
- cached-cookie resume 逻辑依赖 `HandleData.lock` 串行化 `lseek`+`getdents` 对，
  offload 后锁在 pool 线程上生效，语义不变。
- 同类未异步化的 op 一并排查：OPENDIR/RELEASEDIR/LSEEK 等仍在 runtime 线程同步执行。

### ~~async READ/WRITE 去掉 dup/close，改造为 borrowed fd~~（已完成，async/spawn-blocking）

实现：`common/async_file.rs` 增加 `File::Borrowed { fd, _guard }` 变体（guard =
`Arc<HandleData>`，uring 路径临时包装 `tokio_uring::fs::File` 并用 ForgetOnDrop
防止取消时误关 fd）；passthrough `async_file_from_data()` 改为借用。
提交：`d200731`（common）+ `acf94d5`（passthrough），clippy 修复已 fold 进
`4854134`（pipelined poll_handler）。

验证（tmpfs，THREADS=4，warmup+3 轮，borrow vs dup A/B 对比）：
seqwrite +6.0%、seqread +2.5%、randwrite-4k +8.2%、randread-4k +8.1%；
filecreate/filedelete 不受影响（预期内，不走 borrowed 路径）。4k 负载逐轮一致，
收益真实。结论：dup/close 开销已消除；剩余读差距（-62%~-69%）归因于 cached 小读
的 io_uring submit/reap 开销（下一优化点）与服务线程数（Multi-task transport）。

追加 256K 探测：randread-256k sync 3309 vs async 1460 MB/s（-55.9%），
randwrite-256k 1923 vs 1764（-8.3%）——bs 增大仅收窄读差距 6 个百分点，
确认差距含每字节分量，非纯每请求开销。

### Guest direct IO 支持（进行中）

- [x] do_open 协商 FOPEN_DIRECT_IO
- [x] 请求缓冲区页对齐（posix_memalign）
- [x] sync read/write bounce buffer
- [x] async read/write 中继到 sync handler（io_uring 直传未对齐缓冲区会 EINVAL）
- [x] 真实磁盘 DIO 性能对比（sync vs async）——结论：async 单任务全面落后，
      根因是 DIO 同步中继 + 单服务线程，非 io_uring 路径劣势
- [ ] buffered（cache miss）场景 sync vs async 对比，验证 io_uring 原生路径价值
- [ ] 合入前的回归验证（buffered 路径 lib tests + smoke）
