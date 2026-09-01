#!/usr/bin/env python3
# Copyright 2026 Alibaba Cloud. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
"""Microbenchmark: 4k random reads that ALWAYS reach the fuse daemon.

Each read fadvise(DONTNEED)s the target page first, so the read cannot be
served from the fuse page cache. Compares receive-path cost of the daemon
(epoll vs blocking channel) without any kernel-cache interference.

Usage: micro_read_ab.py <mountpoint> <procs> <iters>
"""
import os
import random
import sys
import time

FADV_DONTNEED = 4
BLOCK = 4096


def worker(path, iters, seed, out_fd):
    random.seed(seed)
    blocks = os.path.getsize(path) // BLOCK
    fd = os.open(path, os.O_RDONLY)
    start = time.monotonic()
    for _ in range(iters):
        off = random.randrange(blocks) * BLOCK
        os.posix_fadvise(fd, off, BLOCK, FADV_DONTNEED)
        os.pread(fd, BLOCK, off)
    elapsed = time.monotonic() - start
    os.close(fd)
    os.write(out_fd, b"%f\n" % elapsed)
    os._exit(0)


def main():
    if len(sys.argv) != 4:
        print("usage: micro_read_ab.py <mountpoint> <procs> <iters>")
        sys.exit(2)
    mnt, procs, iters = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

    # Pick the largest regular file in the mountpoint root (top level only).
    target = None
    for root, _dirs, files in os.walk(mnt):
        for f in files:
            p = os.path.join(root, f)
            if target is None or os.path.getsize(p) > os.path.getsize(target):
                target = p
        break
    if target is None:
        print("no test file found under %s" % mnt)
        sys.exit(1)
    # randrange() needs at least one full block; bail out cleanly instead of
    # letting a worker die with an empty-range ValueError, which would then
    # hang the parent's result collection below.
    if os.path.getsize(target) < BLOCK:
        print("target %s is smaller than one %d-byte block" % (target, BLOCK))
        sys.exit(1)

    # Take the wall-clock start before forking so it spans the whole run;
    # starting it after the fork loop would under-count the elapsed time and
    # over-report throughput, since the earliest workers are already running.
    total_start = time.monotonic()
    pipes = []
    for i in range(procs):
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(r)
            worker(target, iters, 42 + i, w)
        os.close(w)
        pipes.append(r)

    # Collect each worker's self-reported elapsed time. The slowest worker
    # bounds the run, so report it (worst, not mean) as the per-op latency.
    worst = 0.0
    for r in pipes:
        data = b""
        while b"\n" not in data:
            chunk = os.read(r, 64)
            if not chunk:  # EOF: the worker died without reporting
                raise RuntimeError("worker on pipe %d died without reporting" % r)
            data += chunk
        worst = max(worst, float(data))
    wall = time.monotonic() - total_start

    ops = procs * iters
    print("procs=%d iters=%d total_ops=%d wall=%.3fs throughput=%.0f ops/s per_proc_worst=%.1f us/op"
          % (procs, iters, ops, wall, ops / wall, worst / iters * 1e6))


if __name__ == "__main__":
    main()
