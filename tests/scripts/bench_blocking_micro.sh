#!/usr/bin/env bash
# Copyright 2026 Alibaba Cloud. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Drive the epoll-vs-blocking fuse channel read microbenchmark.
#
# For each channel type (epoll = the default FuseChannel, blocking = the
# BlockingFuseChannel) and each daemon worker count, mount the passthrough
# test daemon and run micro_read_ab.py with 1 and 4 client processes. Each
# 4k random read fadvise(DONTNEED)s its page first, so every read reaches
# the daemon and the numbers reflect the request receive path rather than
# the kernel page cache.
#
# Requirements: Linux, fusermount3, permission to mount fuse. The daemon is
# built automatically when $BIN does not exist.
#
# Tunables (environment variables):
#   BIN    path of the passthrough daemon (default: build this repo's)
#   WORK   scratch dir for the source data and mountpoint (default mktemp)
#   ITERS  reads per client process (default 40000)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
BIN=${BIN:-"${REPO_DIR}/target/release/passthrough"}
WORK=${WORK:-$(mktemp -d /tmp/fuse-ab-micro.XXXXXX)}
SRC="${WORK}/data"
MNT="${WORK}/mnt"
ITERS=${ITERS:-40000}
mkdir -p "${SRC}" "${MNT}"

# tests/passthrough is a member of the root workspace, so its binary lands
# in the workspace target directory; build it if the caller did not supply
# a prebuilt one via $BIN.
if [ ! -x "${BIN}" ]; then
    echo "building passthrough daemon..."
    (cd "${REPO_DIR}/tests/passthrough" && cargo build --release)
fi
[ -x "${BIN}" ] || { echo "error: daemon ${BIN} missing and build failed" >&2; exit 1; }

DAEMON_PID=
cleanup() {
    if [ -n "${DAEMON_PID}" ]; then
        kill -TERM "${DAEMON_PID}" 2>/dev/null || true
        wait "${DAEMON_PID}" 2>/dev/null || true
    fi
    fusermount3 -u "${MNT}" 2>/dev/null || umount "${MNT}" 2>/dev/null || true
}
trap cleanup EXIT

for blocking in false true; do
    if [ "${blocking}" = "true" ]; then mode=blocking; else mode=epoll; fi
    for threads in 1 2 4; do
        echo "=== mode=${mode} daemon_threads=${threads} ==="
        rm -rf "${SRC:?}"/*
        log="${WORK}/daemon-${mode}-${threads}.log"
        "${BIN}" "${SRC}" "${MNT}" "${threads}" "${blocking}" >"${log}" 2>&1 &
        DAEMON_PID=$!
        # Wait for the mount to appear; fail loudly (dumping the daemon log)
        # rather than silently skipping when the daemon dies on startup.
        mounted=false
        for _ in $(seq 50); do
            if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
                echo "error: daemon died on startup (mode=${mode} threads=${threads}):" >&2
                cat "${log}" >&2
                exit 1
            fi
            if mountpoint -q "${MNT}"; then mounted=true; break; fi
            sleep 0.1
        done
        if [ "${mounted}" != true ]; then
            echo "error: daemon did not mount ${MNT} (mode=${mode} threads=${threads})" >&2
            cat "${log}" >&2
            exit 1
        fi

        # 1GB test file, created through the mount so it lands in SRC.
        dd if=/dev/zero of="${MNT}/testfile" bs=1M count=1024 conv=fsync >/dev/null
        sync
        for procs in 1 4; do
            echo -n "  client_procs=${procs}: "
            python3 "${SCRIPT_DIR}/micro_read_ab.py" "${MNT}" "${procs}" "${ITERS}"
        done

        # SIGTERM makes the daemon umount itself; wait for it, then clear the
        # mount defensively before the next iteration reuses MNT.
        kill -TERM "${DAEMON_PID}" 2>/dev/null || true
        wait "${DAEMON_PID}" 2>/dev/null || true
        DAEMON_PID=
        fusermount3 -u "${MNT}" 2>/dev/null || umount "${MNT}" 2>/dev/null || true
        sleep 1
    done
done
echo "MICRO AB DONE"
