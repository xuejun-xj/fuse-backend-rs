# fuse-backend-rs benchmark tooling

Tooling to compare the performance of the synchronous and asynchronous IO
paths, complementing the functional tests. Two levels are provided:

## 1. Micro-benchmark (framework overhead)

`benches/sync_vs_async_microbench.rs` measures per-operation latency of
`PassthroughFs` through the synchronous `FileSystem` interface versus the
delegated `AsyncFileSystem` interface, without the kernel fuse transport in
the loop.

Covered operations: `lookup`, `getattr`, `open+read(4KB)+release`,
`open+write(4KB)+release`, `create+release+unlink`.

```sh
cd tests/benchmark
cargo bench
```

Note that the async numbers include the runtime dispatch cost
(`Runtime::block_on()` per operation). These numbers quantify the overhead
of delegation itself; they do not predict end-to-end throughput, because the
fuse transport and request concurrency dominate there.

## 2. End-to-end comparison with fio

`tests/scripts/bench_sync_async.sh` mounts the `fuse-backend-rs-benchmark`
daemon twice — once in sync mode (N worker threads, one fuse channel each)
and once in async mode (a single `FuseDevTask` driven by the async runtime,
tokio-uring when io_uring is available) — and runs identical fio workloads:

- sequential read/write with 1MB requests
- random read/write with 4KB requests
- metadata operations (file create/delete)

```sh
sudo tests/scripts/bench_sync_async.sh      # needs fio + fuse mount rights
THREADS=8 RUNTIME=60 sudo -E tests/scripts/bench_sync_async.sh
```

The script builds the daemon in release mode; when invoking it with `sudo`,
build once as a regular user first (`cd tests/benchmark && cargo build
--release`) to avoid root-owned artifacts in the workspace `target/`
directory.

Raw fio output is written to a temporary results directory (printed at the
start), and a side-by-side summary is printed at the end. The default
execution order is sync then async; the second mode benefits from a warmer
page cache, so cross-check with `MODES="async sync"`.

## Interpretation

The async path currently delegates all operations to the synchronous
handlers and processes requests sequentially with a single task/buffer,
while the sync path serves requests from multiple worker threads. Expect
the async mode to be competitive on single-stream latency but behind the
sync mode on highly concurrent workloads; a native io_uring hot path is
tracked as follow-up work (#188).

## Limitations

- Linux only: the `async-io` feature depends on io-uring/tokio.
- The benchmark daemon disables `no_open`/`no_opendir` to exercise the
  full open/handle paths.
