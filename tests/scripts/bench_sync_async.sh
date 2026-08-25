#!/usr/bin/env bash
# Copyright 2026 Alibaba Cloud. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Compare the synchronous and asynchronous fusedev IO paths of
# fuse-backend-rs using fio.
#
# For each mode (sync: N worker threads, async: one FuseDevTask on the
# async runtime), the script mounts the benchmark daemon and runs the same
# fio workloads against the mountpoint. The fio results are stored in JSON
# format ($RESULTS_DIR/<mode>-<workload>.json) under $RESULTS_DIR for
# further analysis, see tests/scripts/bench_compare.py.
#
# Requirements: Linux, fio, jq, permission to mount fuse (root or
# fusermount).
#
# Tunables (environment variables):
#   THREADS  number of sync worker threads / fio jobs (default 4)
#   SIZE     file size for the sequential workloads (default 256M)
#   RUNTIME  seconds per time-based workload (default 30)
#   NRFILES  number of files for the metadata workloads (default 10000)
#   MODES    execution order of the modes (default "sync async"); the
#            second mode benefits from a warmer page cache, so run both
#            orders to cross-check the results
#   RESULTS_DIR where to store results (default a fresh mktemp directory)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
BENCH_DIR="${REPO_DIR}/tests/benchmark"
# tests/benchmark is a member of the root workspace, so build artifacts
# land in the workspace target directory, not under tests/benchmark.
DAEMON="${REPO_DIR}/target/release/fuse-backend-rs-benchmark"

THREADS=${THREADS:-4}
SIZE=${SIZE:-256M}
RUNTIME=${RUNTIME:-30}
NRFILES=${NRFILES:-10000}
MODES=${MODES:-"sync async"}
RESULTS_DIR=${RESULTS_DIR:-$(mktemp -d /tmp/fuse-bench-results.XXXXXX)}
SRC_DIR="${RESULTS_DIR}/source"
MNT_DIR="${RESULTS_DIR}/mount"
mkdir -p "${SRC_DIR}" "${MNT_DIR}"

command -v fio >/dev/null 2>&1 || { echo "error: fio is required" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "error: jq is required" >&2; exit 1; }

# The metadata workloads keep NRFILES files open at once, and the daemon
# holds an open handle per file as well, but the default soft fd limit of
# GitHub-hosted runners (1024) is far too low for that, so raise the soft
# limit to the hard limit; fio otherwise fails with
# "try reducing/setting openfiles".
if ! ulimit -S -n "$(ulimit -H -n)"; then
    echo "warning: could not raise the fd limit" >&2
fi

echo "results directory: ${RESULTS_DIR}"

# Build the benchmark daemon in release mode.
(cd "${BENCH_DIR}" && cargo build --release)

DAEMON_PID=
cleanup() {
    if [ -n "${DAEMON_PID}" ]; then
        kill -TERM "${DAEMON_PID}" 2>/dev/null || true
        wait "${DAEMON_PID}" 2>/dev/null || true
    fi
    umount "${MNT_DIR}" 2>/dev/null || true
}
trap cleanup EXIT

start_daemon() {
    local mode_args=$1
    # shellcheck disable=SC2086
    "${DAEMON}" "${SRC_DIR}" "${MNT_DIR}" ${mode_args} --threads "${THREADS}" &
    DAEMON_PID=$!
    for _ in $(seq 50); do
        if mountpoint -q "${MNT_DIR}"; then
            return 0
        fi
        sleep 0.1
    done
    echo "error: daemon did not mount ${MNT_DIR}" >&2
    exit 1
}

stop_daemon() {
    kill -TERM "${DAEMON_PID}" 2>/dev/null || true
    wait "${DAEMON_PID}" 2>/dev/null || true
    DAEMON_PID=
    sleep 1
}

# run_workload <mode> <name> <fio args...>
# Runs a time-based workload for ${RUNTIME} seconds.
run_workload() {
    local mode=$1
    local name=$2
    shift 2
    echo "--- [${mode}] ${name}: $*"
    fio --name="${name}" --directory="${MNT_DIR}" --group_reporting \
        --time_based --runtime="${RUNTIME}" "$@" \
        --output-format=json \
        --output="${RESULTS_DIR}/${mode}-${name}.json" || {
            echo "error: fio workload ${name} failed" >&2
            exit 1
        }
    # Select the active side like bench_compare.py does: the data
    # workloads are active on exactly one side, and the filecreate and
    # filedelete engines may transfer no data at all, so compare iops.
    jq -r '.jobs[0] | if .read.iops >= .write.iops
           then "  read: bw=\(.read.bw)KiB/s iops=\(.read.iops | round)"
           else "  write: bw=\(.write.bw)KiB/s iops=\(.write.iops | round)"
           end' "${RESULTS_DIR}/${mode}-${name}.json" || true
}

# run_fixed_workload <mode> <name> <fio args...>
# Runs a workload for a fixed amount of work instead of ${RUNTIME} seconds.
# The filecreate/filedelete engines cannot be restarted by --time_based:
# filedelete hits ENOENT on the second round because the files created in
# the first round have already been deleted.
run_fixed_workload() {
    local mode=$1
    local name=$2
    shift 2
    echo "--- [${mode}] ${name}: $*"
    fio --name="${name}" --directory="${MNT_DIR}" --group_reporting "$@" \
        --output-format=json \
        --output="${RESULTS_DIR}/${mode}-${name}.json" || {
            echo "error: fio workload ${name} failed" >&2
            exit 1
        }
    jq -r '.jobs[0] | if .read.iops >= .write.iops
           then "  read: iops=\(.read.iops | round) runtime=\(.read.runtime)ms"
           else "  write: iops=\(.write.iops | round) runtime=\(.write.runtime)ms"
           end' "${RESULTS_DIR}/${mode}-${name}.json" || true
}

# shellcheck disable=SC2086
for mode in ${MODES}; do
    mode_args=""
    [ "${mode}" = "async" ] && mode_args="--async"

    echo ""
    echo "############ mode: ${mode} ############"
    start_daemon "${mode_args}"

    # Sequential IO with large requests (throughput oriented).
    run_workload "${mode}" seqwrite --rw=write --bs=1M --size="${SIZE}" \
        --numjobs="${THREADS}" --ioengine=psync
    run_workload "${mode}" seqread --rw=read --bs=1M --size="${SIZE}" \
        --numjobs="${THREADS}" --ioengine=psync

    # Random IO with small requests (latency/metadata oriented).
    run_workload "${mode}" randwrite-4k --rw=randwrite --bs=4k --size=64M \
        --numjobs="${THREADS}" --ioengine=psync
    run_workload "${mode}" randread-4k --rw=randread --bs=4k --size=64M \
        --numjobs="${THREADS}" --ioengine=psync

    # Metadata operations: create and delete lots of small files. These run
    # for a fixed number of files instead of ${RUNTIME} seconds, see
    # run_fixed_workload().
    run_fixed_workload "${mode}" filecreate --ioengine=filecreate \
        --nrfiles="${NRFILES}" --numjobs=1 --filesize=4K
    run_fixed_workload "${mode}" filedelete --ioengine=filedelete \
        --nrfiles="${NRFILES}" --numjobs=1 --filesize=4K

    stop_daemon
done

# Print a side-by-side summary of the collected results.
summarize() {
    jq -r '.jobs[0] | if .read.iops >= .write.iops
           then "read: bw=\(.read.bw)KiB/s iops=\(.read.iops | round)"
           else "write: bw=\(.write.bw)KiB/s iops=\(.write.iops | round)"
           end' "$1" 2>/dev/null || echo "n/a"
}

echo ""
echo "############ summary ############"
printf "%-32s %-36s %-36s\n" "workload" "sync" "async"
for f in "${RESULTS_DIR}"/sync-*.json; do
    name=$(basename "${f}" .json)
    name=${name#sync-}
    printf "%-32s %-36s %-36s\n" "${name}" \
        "$(summarize "${f}")" \
        "$(summarize "${RESULTS_DIR}/async-${name}.json")"
done
echo ""
echo "full fio output: ${RESULTS_DIR}"
