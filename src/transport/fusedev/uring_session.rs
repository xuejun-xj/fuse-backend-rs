// Copyright (C) 2026 Alibaba Cloud. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! FUSE-over-io_uring serving transport (experimental, kernel 6.14+, protocol 7.42).
//!
//! Requests registered through `IORING_OP_URING_CMD` SQEs on the /dev/fuse file
//! are delivered as CQEs; replies are sent back together with a re-registration
//! through `FUSE_IO_URING_CMD_COMMIT_AND_FETCH`. One kernel queue exists per
//! possible CPU, and each entry is attached to an explicit queue id chosen by
//! the daemon in the SQE command payload.
//!
//! The kernel still uses the classic /dev/fuse path for requests it cannot
//! route through io_uring (FUSE_INTERRUPT and requests queued before entries
//! were registered), so a classic channel thread keeps serving in parallel.
//!
//! Experimental: the kernel interface is still evolving; only the minimal 7.42
//! interface is implemented.

use std::fs::File;
use std::io::{self, IoSlice, Write};
use std::mem::ManuallyDrop;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use vm_memory::ByteValued;

use crate::abi::fuse_abi::{InHeader, OutHeader};
use crate::abi::fuse_uring::{
    FuseUringCmdReq, FuseUringEntInOut, FuseUringReqHeader, FUSE_IO_URING_CMD_COMMIT_AND_FETCH,
    FUSE_IO_URING_CMD_REGISTER, FUSE_URING_IN_OUT_HEADER_SZ, FUSE_URING_IOV_SEGS,
};
use crate::api::filesystem::FileSystem;
use crate::api::server::Server;
use crate::file_buf::FileVolatileSlice;
use crate::file_traits::FileReadWriteVolatile;
use crate::transport::fusedev::{FuseBuf, FuseChannel, FuseSession, FUSE_HEADER_SIZE};
use crate::transport::{Error::*, Reader, Result, Writer};
use crate::BitmapSlice;

/// Size of `fuse_in_header`/`fuse_out_header` on the wire.
const IN_HEADER_SIZE: usize = 40;
const OUT_HEADER_SIZE: usize = 16;

/// Field-for-field mirror of `struct io_uring_sqe`, used to reach the
/// `addr`/`len` fields of an SQE128 which the io-uring crate does not
/// expose for `IORING_OP_URING_CMD` entries.
#[allow(dead_code)]
#[repr(C)]
struct SqeMirror {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    op_flags: u32,
    user_data: u64,
    buf_index: u16,
    personality: u16,
    splice_fd_in: i32,
    addr3: u64,
    pad2: u64,
}

/// Configuration of the io_uring serving transport.
#[derive(Clone, Copy, Debug)]
pub struct UringConfig {
    /// Number of worker threads, each owning one io_uring instance. The
    /// kernel queues are distributed among the workers round-robin.
    /// `0` means one worker per online CPU.
    pub workers: usize,
    /// Number of ring entries registered per kernel queue, i.e. the number
    /// of in-flight requests each queue may hold.
    pub entries_per_queue: usize,
}

impl Default for UringConfig {
    fn default() -> Self {
        UringConfig {
            workers: 0,
            entries_per_queue: 4,
        }
    }
}

/// Parse a CPU mask like "0-3,5,8-11" into the number of possible CPUs,
/// which is the number of kernel fuse queues.
fn parse_cpu_mask(content: &str) -> Result<usize> {
    let mut count = 0usize;
    for part in content.trim().split(',') {
        let range: Vec<&str> = part.split('-').collect();
        let (start, end) = match range.as_slice() {
            [a] => (*a, *a),
            [a, b] => (*a, *b),
            _ => return Err(SessionFailure(format!("invalid cpu mask range: {part}"))),
        };
        let start: usize = start
            .trim()
            .parse()
            .map_err(|_| SessionFailure(format!("invalid cpu mask: {content}")))?;
        let end: usize = end
            .trim()
            .parse()
            .map_err(|_| SessionFailure(format!("invalid cpu mask: {content}")))?;
        if end < start {
            return Err(SessionFailure(format!("invalid cpu mask range: {part}")));
        }
        count += end - start + 1;
    }
    if count == 0 {
        return Err(SessionFailure("empty cpu mask".to_string()));
    }
    Ok(count)
}

/// Read `/sys/devices/system/cpu/possible` to get the number of possible
/// CPUs, which is the number of kernel fuse queues.
fn possible_cpu_count() -> Result<usize> {
    let content = std::fs::read_to_string("/sys/devices/system/cpu/possible")
        .map_err(|e| SessionFailure(format!("read possible cpu mask: {e}")))?;
    parse_cpu_mask(&content)
}

/// Size of the per-opcode request arguments carried in the entry `op_in`
/// slot, for reassembling the classic wire layout. The kernel copies
/// `in_args[0]` (the per-op header) into `op_in` and the remaining
/// arguments into the payload area, so `Some(0)` means a known opcode
/// without a fixed header; `None` marks opcodes the daemon cannot serve.
fn in_args_len(opcode: u32) -> Option<usize> {
    let len = match opcode {
        1 => 0,        // LOOKUP: name only
        2 => 8,        // FORGET: ForgetIn
        3 => 16,       // GETATTR: GetattrIn
        4 => 88,       // SETATTR: SetattrIn
        5 => 0,        // READLINK: no arguments
        6 => 0,        // SYMLINK: name + linkname
        8 => 16,       // MKNOD: MknodIn
        9 => 8,        // MKDIR: MkdirIn
        10 | 11 => 0,  // UNLINK/RMDIR: name only
        12 => 8,       // RENAME: RenameIn
        13 => 8,       // LINK: LinkIn
        14 => 8,       // OPEN: OpenIn
        15 => 40,      // READ: ReadIn
        16 => 40,      // WRITE: WriteIn
        17 => 0,       // STATFS: no arguments
        18 => 24,      // RELEASE: ReleaseIn
        20 => 16,      // FSYNC: FsyncIn
        21 => 8,       // SETXATTR: SetxattrIn
        22 => 8,       // GETXATTR: GetxattrIn
        23 => 8,       // LISTXATTR: GetxattrIn
        24 => 0,       // REMOVEXATTR: name only
        25 => 24,      // FLUSH: FlushIn
        26 => 16,      // INIT: InitIn
        27 => 8,       // OPENDIR: OpenIn
        28 | 44 => 40, // READDIR/READDIRPLUS: ReadIn
        29 => 24,      // RELEASEDIR: ReleaseIn
        30 => 16,      // FSYNCDIR: FsyncIn
        31..=33 => 48, // GETLK/SETLK/SETLKW: LkIn
        34 => 8,       // ACCESS: AccessIn
        35 => 16,      // CREATE: CreateIn
        36 => 8,       // INTERRUPT: InterruptIn
        37 => 16,      // BMAP: BmapIn
        38 => 0,       // DESTROY: no arguments
        39 => 32,      // IOCTL: IoctlIn
        40 => 24,      // POLL: PollIn
        41 => 0,       // NOTIFY_REPLY: payload only
        42 => 8,       // BATCH_FORGET: BatchForgetIn
        43 => 32,      // FALLOCATE: FallocateIn
        45 => 16,      // RENAME2: Rename2In
        46 => 24,      // LSEEK: LseekIn
        47 => 56,      // COPY_FILE_RANGE: CopyFileRangeIn
        // SETUPMAPPING/REMOVEMAPPING only exist on the virtiofs DAX window
        // path and are never delivered through /dev/fuse, and opcodes 0, 7,
        // 19 and beyond 49 are undefined or reserved.
        _ => return None,
    };
    Some(len)
}

/// A buffer-only [Writer] for io_uring replies.
///
/// The reply is fully staged in memory; staging is completed by the caller
/// copying the staged bytes into the ring entry areas, so `commit()` only
/// accounts the staged bytes instead of issuing a device write.
#[derive(Debug, PartialEq, Eq)]
pub struct UringWriter<'a, S: BitmapSlice = ()> {
    buf: ManuallyDrop<Vec<u8>>,
    phantom: std::marker::PhantomData<&'a mut [S]>,
}

impl<'a, S: BitmapSlice> UringWriter<'a, S> {
    /// Construct a new writer staging the reply in `buf`.
    pub fn new(buf: &'a mut [u8]) -> Self {
        let buf = unsafe { Vec::from_raw_parts(buf.as_mut_ptr(), 0, buf.len()) };
        UringWriter {
            buf: ManuallyDrop::new(buf),
            phantom: std::marker::PhantomData,
        }
    }

    /// Split the writer at the given offset, following the [Writer] semantics.
    pub fn split_at(&mut self, offset: usize) -> Result<UringWriter<'a, S>> {
        if self.buf.capacity() < offset {
            return Err(SplitOutOfBounds(offset));
        }

        let (len1, len2) = if self.buf.len() > offset {
            (offset, self.buf.len() - offset)
        } else {
            (self.buf.len(), 0)
        };
        let cap2 = self.buf.capacity() - offset;
        let ptr = self.buf.as_mut_ptr();

        // Safe because both buffers refer to different parts of the same
        // underlying buffer and never overlap.
        self.buf = unsafe { ManuallyDrop::new(Vec::from_raw_parts(ptr, len1, offset)) };
        let buf = unsafe { ManuallyDrop::new(Vec::from_raw_parts(ptr.add(offset), len2, cap2)) };

        Ok(UringWriter {
            buf,
            phantom: std::marker::PhantomData,
        })
    }

    /// Return the number of bytes already staged.
    pub fn bytes_written(&self) -> usize {
        self.buf.len()
    }

    /// Return the number of bytes available for staging.
    pub fn available_bytes(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }

    /// Account the staged bytes of self and `other`.
    pub fn commit(&mut self, other: Option<&Writer<'a, S>>) -> io::Result<usize> {
        let o = match other {
            Some(Writer::Uring(w)) => w.bytes_written(),
            _ => 0,
        };
        Ok(self.buf.len() + o)
    }

    /// Write an object to the writer.
    pub fn write_obj<T: ByteValued>(&mut self, val: T) -> io::Result<()> {
        self.write_all(val.as_slice())
    }

    /// Stage data read from a file descriptor at offset `off`.
    pub fn write_from_at<F: FileReadWriteVolatile>(
        &mut self,
        mut src: F,
        count: usize,
        off: u64,
    ) -> io::Result<usize> {
        self.check_available_space(count)?;

        let cnt = src.read_vectored_at_volatile(
            // Safe because check_available_space() ensures the capacity.
            unsafe {
                &[FileVolatileSlice::from_raw_ptr(
                    self.buf.as_mut_ptr().add(self.buf.len()),
                    count,
                )]
            },
            off,
        )?;
        let new_len = self.buf.len() + cnt;
        // Safe because cnt <= count was checked above.
        unsafe { self.buf.set_len(new_len) };
        Ok(cnt)
    }

    fn check_available_space(&self, sz: usize) -> io::Result<()> {
        if sz > self.available_bytes() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "data out of range, available {} requested {}",
                    self.available_bytes(),
                    sz
                ),
            ))
        } else {
            Ok(())
        }
    }
}

impl<S: BitmapSlice> Write for UringWriter<'_, S> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.check_available_space(data.len())?;
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let total = bufs.iter().fold(0, |acc, b| acc + b.len());
        self.check_available_space(total)?;
        let count = bufs.iter().filter(|b| !b.is_empty()).fold(0, |acc, b| {
            self.buf.extend_from_slice(b);
            acc + b.len()
        });
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// The buffer is owned by the caller; UringWriter only holds a `ManuallyDrop`
// Vec shell over it, so dropping the writer never frees the memory.

/// One ring entry: the memory areas the kernel reads/writes plus scratch
/// regions used to reassemble requests and collect replies.
struct UringEntry {
    qid: u16,
    /// Single allocation, laid out as
    /// [FuseUringReqHeader][payload][request scratch][reply scratch].
    mem: Box<[u8]>,
    payload_cap: usize,
    /// iovec array referenced by the SQEs, must stay at a stable address.
    iovecs: Box<[libc::iovec; FUSE_URING_IOV_SEGS]>,
}

impl UringEntry {
    fn new(qid: u16, payload_cap: usize) -> UringEntry {
        let scratch = FUSE_HEADER_SIZE + payload_cap;
        let total = std::mem::size_of::<FuseUringReqHeader>() + payload_cap + 2 * scratch;
        let mem = vec![0u8; total].into_boxed_slice();
        let base = mem.as_ptr();
        let iovecs = Box::new([
            libc::iovec {
                iov_base: base as *mut libc::c_void,
                iov_len: std::mem::size_of::<FuseUringReqHeader>(),
            },
            libc::iovec {
                iov_base: unsafe { base.add(std::mem::size_of::<FuseUringReqHeader>()) }
                    as *mut libc::c_void,
                iov_len: payload_cap,
            },
        ]);
        UringEntry {
            qid,
            mem,
            payload_cap,
            iovecs,
        }
    }

    fn header(&self) -> &FuseUringReqHeader {
        let ptr = self.mem.as_ptr() as *const FuseUringReqHeader;
        // Safe because the allocation starts with an aligned FuseUringReqHeader.
        unsafe { &*ptr }
    }

    fn header_mut(&mut self) -> &mut FuseUringReqHeader {
        let ptr = self.mem.as_mut_ptr() as *mut FuseUringReqHeader;
        // Safe because the allocation starts with an aligned FuseUringReqHeader.
        unsafe { &mut *ptr }
    }

    /// Build the `FUSE_IO_URING_CMD_REGISTER`/`COMMIT_AND_FETCH` SQE for this
    /// entry, with `commit_id` set in the command payload.
    fn cmd_sqe(&self, fd: i32, cmd_op: u32, commit_id: u64, user_data: u64) -> squeue::Entry128 {
        let cmd = FuseUringCmdReq {
            qid: self.qid,
            commit_id,
            ..Default::default()
        };
        let mut bytes = [0u8; 80];
        bytes[..std::mem::size_of::<FuseUringCmdReq>()].copy_from_slice(cmd.as_slice());

        let mut sqe = opcode::UringCmd80::new(types::Fd(fd), cmd_op)
            .cmd(bytes)
            .build();
        // sqe->addr points to the iovec array and sqe->len to the segment
        // count, as required by fuse_uring_get_iovec_from_sqe(). The crate
        // exposes no accessor for them on URING_CMD entries, so patch them
        // through a layout mirror of the SQE.
        //
        // Safe because SqeMirror mirrors the kernel ABI layout of
        // io_uring_sqe and Entry128 is an SQE plus a 64-byte command area;
        // the sizes are asserted below and only addr/len are overwritten.
        unsafe {
            assert_eq!(
                std::mem::size_of::<(SqeMirror, [u8; 64])>(),
                std::mem::size_of::<squeue::Entry128>()
            );
            let (mut inner, cmd_area) =
                std::mem::transmute::<squeue::Entry128, (SqeMirror, [u8; 64])>(sqe);
            inner.addr = self.iovecs.as_ptr() as u64;
            inner.len = FUSE_URING_IOV_SEGS as u32;
            sqe = std::mem::transmute::<(SqeMirror, [u8; 64]), squeue::Entry128>((inner, cmd_area));
        }
        sqe.user_data(user_data)
    }

    /// Stage a `-EIO` error reply after a failed `process()`, so that the
    /// subsequent `COMMIT_AND_FETCH` hands the client a proper error instead
    /// of the request data still sitting in the entry areas.
    fn stage_error_reply(&mut self) {
        let header = self.header_mut();
        let mut in_hdr = InHeader::default();
        in_hdr
            .as_mut_slice()
            .copy_from_slice(&header.in_out[..IN_HEADER_SIZE]);
        let out = OutHeader {
            len: OUT_HEADER_SIZE as u32,
            error: -libc::EIO,
            unique: in_hdr.unique,
        };
        header.in_out[..OUT_HEADER_SIZE].copy_from_slice(out.as_slice());
        header.ring_ent_in_out.payload_sz = 0;
    }
}

/// One io_uring based serving thread, responsible for a subset of the kernel
/// queues.
struct UringWorker<F: FileSystem + Send + Sync + 'static> {
    fd: File,
    qids: Vec<u16>,
    entries_per_queue: usize,
    payload_cap: usize,
    server: Arc<Server<F>>,
}

impl<F: FileSystem + Send + Sync + 'static> UringWorker<F> {
    fn run(self, exit: Arc<AtomicBool>, ready_tx: Sender<io::Result<()>>) -> io::Result<()> {
        let total_entries = self.qids.len() * self.entries_per_queue;
        let mut ring: IoUring<squeue::Entry128, cqueue::Entry> =
            IoUring::<squeue::Entry128, cqueue::Entry>::generic_builder()
                .build((total_entries * 2) as u32)?;
        let fd = self.fd.as_raw_fd();

        let mut entries: Vec<UringEntry> = Vec::with_capacity(total_entries);
        for qid in &self.qids {
            for _ in 0..self.entries_per_queue {
                entries.push(UringEntry::new(*qid, self.payload_cap));
            }
        }

        // Register all entries; the CQE of each REGISTER SQE returns once a
        // request has been assigned to the entry.
        let registered = (|| {
            let mut sq = ring.submission();
            for (idx, entry) in entries.iter().enumerate() {
                let sqe = entry.cmd_sqe(fd, FUSE_IO_URING_CMD_REGISTER, 0, idx as u64);
                // Safe because entries live for the whole lifetime of the loop.
                unsafe { sq.push(&sqe) }
                    .map_err(|e| io::Error::other(format!("submission queue full: {e}")))?;
            }
            drop(sq);
            ring.submit()
        })()
        .map(|_| ());
        let _ = ready_tx.send(match &registered {
            Ok(()) => Ok(()),
            Err(e) => Err(io::Error::other(e.to_string())),
        });
        registered?;

        loop {
            ring.submit_and_wait(1)?;

            // Drain the completion queue first so that its borrow is
            // released before entries are processed and commits submitted.
            let completed: Vec<(usize, i32)> = ring
                .completion()
                .map(|cqe| (cqe.user_data() as usize, cqe.result()))
                .collect();
            if completed.is_empty() {
                continue;
            }

            for (idx, res) in completed {
                if idx >= entries.len() {
                    warn!("uring worker: CQE with unexpected user_data {}", idx);
                    continue;
                }
                let entry = &mut entries[idx];
                if res < 0 {
                    if exit.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    // Registration/commit failures leave the entry in an
                    // undefined state; stop serving rather than guessing.
                    return Err(io::Error::from_raw_os_error(-res));
                }

                if let Err(e) = self.process(entry) {
                    warn!("uring worker: failed to handle request: {e}");
                    entry.stage_error_reply();
                }

                let ent: FuseUringEntInOut = entry.header().ring_ent_in_out;
                let sqe = entry.cmd_sqe(
                    fd,
                    FUSE_IO_URING_CMD_COMMIT_AND_FETCH,
                    ent.commit_id,
                    idx as u64,
                );
                loop {
                    let pushed = {
                        let mut sq = ring.submission();
                        // Safe because the entry outlives the ring usage.
                        unsafe { sq.push(&sqe) }.is_ok()
                    };
                    if pushed {
                        break;
                    }
                    // The submission queue can never hold more outstanding
                    // commits than completed entries, so submitting must
                    // eventually make room.
                    ring.submit()?;
                }
            }
            ring.submit()?;
        }
    }

    /// Handle one delivered request: reassemble the classic wire layout,
    /// dispatch to the server and stage the reply into the entry areas.
    fn process(&self, entry: &mut UringEntry) -> Result<()> {
        // Copy the request parts out first, so that the reply scratch region
        // can be borrowed mutably afterwards without aliasing.
        let in_header: InHeader = {
            let header = entry.header();
            let mut hdr = InHeader::default();
            hdr.as_mut_slice()
                .copy_from_slice(&header.in_out[..IN_HEADER_SIZE]);
            hdr
        };
        let args_len = in_args_len(in_header.opcode).ok_or_else(|| {
            SessionFailure(format!(
                "uring: unsupported opcode {} for queue {}",
                in_header.opcode, entry.qid
            ))
        })?;
        let payload_sz = entry.header().ring_ent_in_out.payload_sz as usize;
        if args_len > FUSE_URING_IN_OUT_HEADER_SZ || payload_sz > entry.payload_cap {
            return Err(SessionFailure(format!(
                "uring: malformed request, opcode {} args {} payload {}",
                in_header.opcode, args_len, payload_sz
            )));
        }

        let msg_len = IN_HEADER_SIZE + args_len + payload_sz;
        let header_len = std::mem::size_of::<FuseUringReqHeader>();
        let scratch = FUSE_HEADER_SIZE + entry.payload_cap;
        let base = entry.mem.as_mut_ptr();

        // Reassemble the classic contiguous layout in the request scratch
        // region: fuse_in_header + per-opcode arguments + payload.
        //
        // Safe because all slices below address disjoint regions of the
        // entry allocation and live for the duration of this function only.
        unsafe {
            let header = &*(base as *const FuseUringReqHeader);
            let req =
                std::slice::from_raw_parts_mut(base.add(header_len + entry.payload_cap), scratch);
            let payload = std::slice::from_raw_parts(base.add(header_len), entry.payload_cap);
            let mut off = 0;
            req[off..off + IN_HEADER_SIZE].copy_from_slice(&header.in_out[..IN_HEADER_SIZE]);
            off += IN_HEADER_SIZE;
            req[off..off + args_len].copy_from_slice(&header.op_in[..args_len]);
            off += args_len;
            req[off..off + payload_sz].copy_from_slice(&payload[..payload_sz]);
            // in_header.len describes the classic contiguous message.
            req[..4].copy_from_slice(&(msg_len as u32).to_le_bytes());
        }

        let total = {
            // Safe because the request and reply scratch regions are disjoint
            // and no other references to the entry exist in this scope.
            let reader = unsafe {
                let req = std::slice::from_raw_parts_mut(
                    base.add(header_len + entry.payload_cap),
                    msg_len,
                );
                Reader::<()>::from_fuse_buffer(FuseBuf::new(req))?
            };
            let writer = unsafe {
                let reply = std::slice::from_raw_parts_mut(
                    base.add(header_len + entry.payload_cap + scratch),
                    scratch,
                );
                UringWriter::new(reply)
            };
            self.server
                .handle_message(reader, Writer::Uring(writer), None, None)
                .map_err(|e| {
                    SessionFailure(format!(
                        "uring: handle message unique 0x{:x}: {}",
                        in_header.unique, e
                    ))
                })?
        };

        if total > 0 {
            // The staged reply must fit the entry header slot plus the
            // payload area; anything larger cannot be committed.
            if total < OUT_HEADER_SIZE || total > OUT_HEADER_SIZE + entry.payload_cap {
                return Err(SessionFailure(format!(
                    "uring: malformed reply of {} bytes for unique 0x{:x}",
                    total, in_header.unique
                )));
            }
            // The staged reply starts with fuse_out_header; move it into the
            // entry header slot and stage the rest in the payload area.
            //
            // Safe because all slices below address disjoint regions of the
            // entry allocation.
            unsafe {
                let header = &mut *(base as *mut FuseUringReqHeader);
                let reply = std::slice::from_raw_parts(
                    base.add(header_len + entry.payload_cap + scratch),
                    total,
                );
                header.in_out[..OUT_HEADER_SIZE].copy_from_slice(&reply[..OUT_HEADER_SIZE]);
                let payload_len = total - OUT_HEADER_SIZE;
                let payload =
                    std::slice::from_raw_parts_mut(base.add(header_len), entry.payload_cap);
                payload[..payload_len].copy_from_slice(&reply[OUT_HEADER_SIZE..]);
                header.ring_ent_in_out.payload_sz = payload_len as u32;
            }
        } else {
            // Forget-like requests carry no reply.
            entry.header_mut().ring_ent_in_out.payload_sz = 0;
        }

        Ok(())
    }
}

/// Serve FUSE requests over io_uring (experimental).
///
/// The serving layer takes ownership of the mounted session: dropping it
/// unmounts the filesystem and joins all serving threads.
pub struct UringFuseServing<F: FileSystem + Send + Sync + 'static> {
    session: FuseSession,
    exit: Arc<AtomicBool>,
    workers: Vec<JoinHandle<io::Result<()>>>,
    fallback: Option<JoinHandle<()>>,
    _fs: std::marker::PhantomData<F>,
}

impl<F: FileSystem + Send + Sync + 'static> UringFuseServing<F> {
    /// Create a new io_uring serving transport on an already mounted session.
    ///
    /// The `FUSE_OVER_IO_URING` capability must have been requested through
    /// `Server::set_uring()` before mounting. This constructor consumes the
    /// INIT request on a classic channel and verifies that the kernel
    /// accepted the capability, returning `Error::UringNotSupported`
    /// otherwise so that the caller can fall back to the classic transport.
    pub fn new(
        session: FuseSession,
        server: Arc<Server<F>>,
        cfg: UringConfig,
    ) -> Result<UringFuseServing<F>> {
        warn!("FUSE-over-io_uring transport is experimental, the kernel interface may change");

        let payload_cap = session.bufsize().saturating_sub(FUSE_HEADER_SIZE);
        let nr_queues = possible_cpu_count()?;

        // The first message on a new connection is always FUSE_INIT; handle
        // it on a classic channel and verify the negotiation outcome.
        let mut init_ch = session.new_channel()?;
        let fallback_ch = session.new_channel()?;
        match init_ch.get_request() {
            Ok(Some((reader, writer))) => {
                server
                    .handle_message(reader, Writer::FuseDev(writer), None, None)
                    .map_err(|e| SessionFailure(format!("uring: INIT failed: {e}")))?;
            }
            Ok(None) => {
                return Err(SessionFailure(
                    "uring: session interrupted during INIT".into(),
                ))
            }
            Err(e) => return Err(SessionFailure(format!("uring: INIT read failed: {e}"))),
        }
        if !server.uring_enabled() {
            return Err(UringNotSupported);
        }

        let workers = if cfg.workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            cfg.workers
        };
        let workers = workers.min(nr_queues);

        let mut queue_ids: Vec<Vec<u16>> = vec![Vec::new(); workers];
        for qid in 0..nr_queues {
            queue_ids[qid % workers].push(qid as u16);
        }

        let file = session
            .get_fuse_file()
            .ok_or_else(|| SessionFailure("uring: session not mounted".into()))?
            .try_clone()
            .map_err(|e| SessionFailure(format!("uring: dup fuse fd: {e}")))?;

        let exit = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = channel();
        let mut handles = Vec::with_capacity(workers);
        for ids in queue_ids {
            let worker = UringWorker {
                fd: file
                    .try_clone()
                    .map_err(|e| SessionFailure(format!("uring: dup fuse fd: {e}")))?,
                qids: ids,
                entries_per_queue: cfg.entries_per_queue,
                payload_cap,
                server: server.clone(),
            };
            let handle = thread::Builder::new()
                .name(format!("uring-{}", handles.len()))
                .spawn({
                    let exit = exit.clone();
                    let ready_tx = ready_tx.clone();
                    move || worker.run(exit, ready_tx)
                })
                .map_err(|e| SessionFailure(format!("uring: spawn worker: {e}")))?;
            handles.push(handle);
        }
        drop(ready_tx);

        // Wait until all entries are registered, bailing out on the first
        // worker failure (e.g. the fuse module parameter disabled uring).
        for _ in 0..workers {
            match ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return Err(SessionFailure(format!(
                        "uring: entry registration failed: {e}"
                    )));
                }
                Err(_) => {
                    return Err(SessionFailure(
                        "uring: worker exited during registration".to_string(),
                    ));
                }
            }
        }
        let live = handles;

        let fallback = thread::Builder::new()
            .name("uring-fallback".to_string())
            .spawn({
                let exit = exit.clone();
                move || Self::fallback_loop(fallback_ch, server, exit)
            })
            .map_err(|e| SessionFailure(format!("uring: spawn fallback thread: {e}")))?;

        Ok(UringFuseServing {
            session,
            exit,
            workers: live,
            fallback: Some(fallback),
            _fs: std::marker::PhantomData,
        })
    }

    fn fallback_loop(mut ch: FuseChannel, server: Arc<Server<F>>, exit: Arc<AtomicBool>) {
        loop {
            match ch.get_request() {
                Ok(Some((reader, writer))) => {
                    if let Err(e) =
                        server.handle_message(reader, Writer::FuseDev(writer), None, None)
                    {
                        match e {
                            // The kernel has shut down this session.
                            crate::Error::EncodeMessage(ref err)
                                if err.raw_os_error() == Some(libc::EBADF) =>
                            {
                                break
                            }
                            _ => {
                                warn!("uring fallback: failed to handle message: {e}");
                                continue;
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
            if exit.load(Ordering::Relaxed) {
                break;
            }
        }
    }
}

impl<F: FileSystem + Send + Sync + 'static> Drop for UringFuseServing<F> {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Relaxed);
        let _ = self.session.wake();
        // Tearing down the connection completes all outstanding ring entries
        // with an error, which unblocks the worker threads.
        let _ = self.session.umount();
        if let Some(handle) = self.fallback.take() {
            let _ = handle.join();
        }
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use vmm_sys_util::tempdir::TempDir;

    use crate::abi::fuse_abi::{
        AccessIn, AttrOut, BatchForgetIn, BmapIn, CopyFileRangeIn, CreateIn, EntryOut, FallocateIn,
        FlushIn, ForgetIn, FsyncIn, GetattrIn, GetxattrIn, InHeader, InitIn, InterruptIn, IoctlIn,
        LinkIn, LkIn, LseekIn, MkdirIn, MknodIn, Opcode, OpenIn, OutHeader, PollIn, ReadIn,
        ReleaseIn, Rename2In, RenameIn, SetattrIn, SetxattrIn, WriteIn, ROOT_ID,
    };
    use crate::api::filesystem::FsOptions;
    use crate::passthrough::{Config, PassthroughFs};

    #[test]
    fn test_parse_cpu_mask() {
        assert_eq!(parse_cpu_mask("0").unwrap(), 1);
        assert_eq!(parse_cpu_mask("0-3\n").unwrap(), 4);
        assert_eq!(parse_cpu_mask("0-3,5,8-11").unwrap(), 9);
        assert!(parse_cpu_mask("").is_err());
        assert!(parse_cpu_mask("3-1").is_err());
        assert!(parse_cpu_mask("a-b").is_err());
        assert!(parse_cpu_mask("1-2-3").is_err());
        assert!(possible_cpu_count().unwrap() >= 1);
    }

    #[test]
    fn test_in_args_len() {
        // Per-opcode argument sizes, derived from the *In structures of the
        // fuse ABI (which mirror the kernel's first in_arg per opcode) and
        // from the server-side request decoders.
        let expected: &[(u32, usize)] = &[
            (Opcode::Lookup as u32, 0),
            (Opcode::Forget as u32, std::mem::size_of::<ForgetIn>()),
            (Opcode::Getattr as u32, std::mem::size_of::<GetattrIn>()),
            (Opcode::Setattr as u32, std::mem::size_of::<SetattrIn>()),
            (Opcode::Readlink as u32, 0),
            (Opcode::Symlink as u32, 0),
            (Opcode::Mknod as u32, std::mem::size_of::<MknodIn>()),
            (Opcode::Mkdir as u32, std::mem::size_of::<MkdirIn>()),
            (Opcode::Unlink as u32, 0),
            (Opcode::Rmdir as u32, 0),
            (Opcode::Rename as u32, std::mem::size_of::<RenameIn>()),
            (Opcode::Link as u32, std::mem::size_of::<LinkIn>()),
            (Opcode::Open as u32, std::mem::size_of::<OpenIn>()),
            (Opcode::Read as u32, std::mem::size_of::<ReadIn>()),
            (Opcode::Write as u32, std::mem::size_of::<WriteIn>()),
            (Opcode::Statfs as u32, 0),
            (Opcode::Release as u32, std::mem::size_of::<ReleaseIn>()),
            (Opcode::Fsync as u32, std::mem::size_of::<FsyncIn>()),
            (Opcode::Setxattr as u32, std::mem::size_of::<SetxattrIn>()),
            (Opcode::Getxattr as u32, std::mem::size_of::<GetxattrIn>()),
            (Opcode::Listxattr as u32, std::mem::size_of::<GetxattrIn>()),
            (Opcode::Removexattr as u32, 0),
            (Opcode::Flush as u32, std::mem::size_of::<FlushIn>()),
            (Opcode::Init as u32, std::mem::size_of::<InitIn>()),
            (Opcode::Opendir as u32, std::mem::size_of::<OpenIn>()),
            (Opcode::Readdir as u32, std::mem::size_of::<ReadIn>()),
            (Opcode::Releasedir as u32, std::mem::size_of::<ReleaseIn>()),
            (Opcode::Fsyncdir as u32, std::mem::size_of::<FsyncIn>()),
            (Opcode::Getlk as u32, std::mem::size_of::<LkIn>()),
            (Opcode::Setlk as u32, std::mem::size_of::<LkIn>()),
            (Opcode::Setlkw as u32, std::mem::size_of::<LkIn>()),
            (Opcode::Access as u32, std::mem::size_of::<AccessIn>()),
            (Opcode::Create as u32, std::mem::size_of::<CreateIn>()),
            (Opcode::Interrupt as u32, std::mem::size_of::<InterruptIn>()),
            (Opcode::Bmap as u32, std::mem::size_of::<BmapIn>()),
            (Opcode::Destroy as u32, 0),
            (Opcode::Ioctl as u32, std::mem::size_of::<IoctlIn>()),
            (Opcode::Poll as u32, std::mem::size_of::<PollIn>()),
            (Opcode::NotifyReply as u32, 0),
            (
                Opcode::BatchForget as u32,
                std::mem::size_of::<BatchForgetIn>(),
            ),
            (Opcode::Fallocate as u32, std::mem::size_of::<FallocateIn>()),
            (Opcode::Readdirplus as u32, std::mem::size_of::<ReadIn>()),
            (Opcode::Rename2 as u32, std::mem::size_of::<Rename2In>()),
            (Opcode::Lseek as u32, std::mem::size_of::<LseekIn>()),
            (
                Opcode::CopyFileRange as u32,
                std::mem::size_of::<CopyFileRangeIn>(),
            ),
        ];
        for &(op, len) in expected {
            assert_eq!(in_args_len(op), Some(len), "opcode {}", op);
        }
        // Everything else (holes, virtiofs-only and undefined opcodes)
        // must be rejected.
        for op in 0..=64u32 {
            if !expected.iter().any(|&(e, _)| e == op) {
                assert_eq!(in_args_len(op), None, "opcode {}", op);
            }
        }
        assert_eq!(in_args_len(u32::MAX), None);
    }

    #[test]
    fn test_uring_entry_layout() {
        let payload_cap = 4096;
        let entry = UringEntry::new(3, payload_cap);
        assert_eq!(entry.qid, 3);

        let header_len = std::mem::size_of::<FuseUringReqHeader>();
        let total = header_len + payload_cap + 2 * (FUSE_HEADER_SIZE + payload_cap);
        assert_eq!(entry.mem.len(), total);

        // The iovec array must describe the header and payload areas of the
        // same allocation, at stable addresses.
        let base = entry.mem.as_ptr() as usize;
        assert_eq!(entry.iovecs[0].iov_base as usize, base);
        assert_eq!(entry.iovecs[0].iov_len, header_len);
        assert_eq!(entry.iovecs[1].iov_base as usize, base + header_len);
        assert_eq!(entry.iovecs[1].iov_len, payload_cap);

        // All slots of the header must be reachable through the pointers.
        assert_eq!(entry.header().ring_ent_in_out.payload_sz, 0);
    }

    #[test]
    fn test_cmd_sqe() {
        let entry = UringEntry::new(7, 64);
        let sqe = entry.cmd_sqe(42, FUSE_IO_URING_CMD_REGISTER, 0xdead_beef, 5);

        assert_eq!(
            std::mem::size_of::<(SqeMirror, [u8; 64])>(),
            std::mem::size_of::<squeue::Entry128>()
        );
        // Safe because of the size assertion above.
        let (inner, extra): (SqeMirror, [u8; 64]) = unsafe { std::mem::transmute(sqe) };

        // IORING_OP_URING_CMD as of kernel ABI 6.14.
        assert_eq!(inner.opcode, 46);
        assert_eq!(inner.fd, 42);
        assert_eq!(inner.addr, entry.iovecs.as_ptr() as u64);
        assert_eq!(inner.len, FUSE_URING_IOV_SEGS as u32);
        assert_eq!(inner.user_data, 5);

        // The io-uring crate splits the 80-byte command payload of an
        // SQE128: the first 16 bytes are placed in the sqe cmd union
        // (the addr3/__pad2 slots), the remaining 64 bytes in the extra
        // area, so the payload is contiguous at sqe offsets 48..128 and
        // io_uring_sqe128_cmd() reads it in one piece.
        let mut cmd_bytes = [0u8; 80];
        cmd_bytes[0..8].copy_from_slice(&inner.addr3.to_le_bytes());
        cmd_bytes[8..16].copy_from_slice(&inner.pad2.to_le_bytes());
        cmd_bytes[16..80].copy_from_slice(&extra);

        // The command payload must carry the queue id and commit id.
        let cmd = FuseUringCmdReq {
            qid: 7,
            commit_id: 0xdead_beef,
            ..Default::default()
        };
        assert_eq!(
            cmd_bytes[..std::mem::size_of::<FuseUringCmdReq>()],
            *cmd.as_slice()
        );
    }

    #[test]
    fn test_uring_writer_basic() {
        let mut scratch = vec![0u8; 128];
        let mut w = UringWriter::<'_, ()>::new(&mut scratch);
        assert_eq!(w.bytes_written(), 0);
        assert_eq!(w.available_bytes(), 128);

        w.write_obj(0x1234_5678u32).unwrap();
        w.write_all(&[1u8, 2, 3]).unwrap();
        assert_eq!(w.bytes_written(), 7);
        assert_eq!(w.available_bytes(), 121);

        // Writes beyond the available capacity must fail without
        // overflowing the buffer.
        let big = vec![0u8; 200];
        assert!(w.write(&big).is_err());
        assert!(w.write_all(&big).is_err());
        assert_eq!(w.bytes_written(), 7);
        assert_eq!(&scratch[..7], &[0x78, 0x56, 0x34, 0x12, 1, 2, 3]);
    }

    #[test]
    fn test_uring_writer_split_commit() {
        let mut scratch = vec![0u8; 64];
        let mut w = UringWriter::<'_, ()>::new(&mut scratch);
        w.write_all(&[1u8; 32]).unwrap();

        let mut tail = w.split_at(16).unwrap();
        assert_eq!(w.bytes_written(), 16);
        assert_eq!(w.available_bytes(), 0);
        assert_eq!(tail.bytes_written(), 16);
        assert_eq!(tail.available_bytes(), 32);
        tail.write_all(&[2u8; 8]).unwrap();

        // commit() only accounts the staged bytes, it never touches a
        // device.
        let other = Writer::Uring(tail);
        assert_eq!(w.commit(Some(&other)).unwrap(), 40);
        // Splitting beyond the capacity is rejected.
        assert!(w.split_at(17).is_err());

        // The staged data must still be visible in the buffer.
        assert_eq!(&scratch[..16], &[1u8; 16]);
        assert_eq!(&scratch[16..32], &[1u8; 16]);
        assert_eq!(&scratch[32..40], &[2u8; 8]);
    }

    #[test]
    fn test_uring_writer_write_from_at() {
        let dir = TempDir::new().unwrap();
        let path = dir.as_path().join("data");
        std::fs::write(&path, (0..64).collect::<Vec<u8>>()).unwrap();

        let mut scratch = vec![0u8; 32];
        let mut w = UringWriter::<'_, ()>::new(&mut scratch);
        let file = File::open(&path).unwrap();
        assert_eq!(
            w.write_from_at(file.try_clone().unwrap(), 16, 8).unwrap(),
            16
        );
        assert_eq!(w.bytes_written(), 16);

        // More data than available space must be rejected.
        assert!(w.write_from_at(file, 32, 0).is_err());
        assert_eq!(&scratch[..16], &(8..24).collect::<Vec<u8>>());
    }

    /// A worker over PassthroughFs, for exercising process() without a
    /// kernel supporting FUSE_URING.
    fn make_worker() -> (UringWorker<Arc<PassthroughFs<()>>>, TempDir) {
        let source = TempDir::new().unwrap();
        let cfg = Config {
            root_dir: source.as_path().to_str().unwrap().to_string(),
            do_import: true,
            ..Default::default()
        };
        let fs = PassthroughFs::<()>::new(cfg).unwrap();
        fs.import().unwrap();
        fs.init(FsOptions::all()).unwrap();

        let worker = UringWorker {
            fd: File::open("/dev/null").unwrap(),
            qids: vec![0],
            entries_per_queue: 1,
            payload_cap: 4096,
            server: Arc::new(Server::new(Arc::new(fs))),
        };
        (worker, source)
    }

    /// Plant a request into the entry header/payload areas the way the
    /// kernel does.
    fn plant_request(entry: &mut UringEntry, opcode: u32, args: &[u8], payload: &[u8]) {
        // Safe because getuid/getgid/getpid are always valid.
        let (uid, gid, pid) = unsafe { (libc::getuid(), libc::getgid(), libc::getpid() as u32) };
        let hdr = InHeader {
            len: (IN_HEADER_SIZE + args.len() + payload.len()) as u32,
            opcode,
            unique: 2,
            nodeid: ROOT_ID,
            uid,
            gid,
            pid,
            ..Default::default()
        };

        let header = entry.header_mut();
        header.in_out[..IN_HEADER_SIZE].copy_from_slice(hdr.as_slice());
        header.op_in[..args.len()].copy_from_slice(args);
        let base = entry.mem.as_mut_ptr();
        // Safe because the payload area is a disjoint part of the entry
        // allocation and payload fits into payload_cap.
        unsafe {
            let header_len = std::mem::size_of::<FuseUringReqHeader>();
            std::ptr::copy_nonoverlapping(payload.as_ptr(), base.add(header_len), payload.len());
        }
        entry.header_mut().ring_ent_in_out.payload_sz = payload.len() as u32;
        entry.header_mut().ring_ent_in_out.commit_id = 2;
    }

    fn parse_out_header(entry: &UringEntry) -> OutHeader {
        let mut out = OutHeader::default();
        out.as_mut_slice()
            .copy_from_slice(&entry.header().in_out[..OUT_HEADER_SIZE]);
        out
    }

    #[test]
    fn test_process_getattr() {
        let (worker, _source) = make_worker();
        let mut entry = UringEntry::new(0, worker.payload_cap);

        let args = GetattrIn::default();
        plant_request(&mut entry, Opcode::Getattr as u32, args.as_slice(), &[]);
        worker.process(&mut entry).unwrap();

        let out = parse_out_header(&entry);
        assert_eq!(out.error, 0);
        assert_eq!(out.unique, 2);
        // The reply body is staged in the payload area behind the
        // fuse_out_header slot.
        assert_eq!(
            entry.header().ring_ent_in_out.payload_sz as usize,
            std::mem::size_of::<AttrOut>()
        );
        assert_eq!(entry.header().ring_ent_in_out.commit_id, 2);
    }

    #[test]
    fn test_process_lookup_payload_only() {
        let (worker, _source) = make_worker();
        let mut entry = UringEntry::new(0, worker.payload_cap);

        // LOOKUP carries no fixed arguments, only the name in the payload.
        plant_request(&mut entry, Opcode::Lookup as u32, &[], b"fuse-file\0");
        // Lookup fails with ENOENT on the empty directory, which still
        // exercises the payload reassembly; the reply must be an error.
        worker.process(&mut entry).unwrap();
        let out = parse_out_header(&entry);
        assert_eq!(out.error, -libc::ENOENT);
        assert_eq!(entry.header().ring_ent_in_out.payload_sz, 0);

        // And succeeds once the file exists.
        std::fs::write(_source.as_path().join("fuse-file"), "x").unwrap();
        plant_request(&mut entry, Opcode::Lookup as u32, &[], b"fuse-file\0");
        worker.process(&mut entry).unwrap();
        let out = parse_out_header(&entry);
        assert_eq!(out.error, 0);
        assert_eq!(
            entry.header().ring_ent_in_out.payload_sz as usize,
            std::mem::size_of::<EntryOut>()
        );
    }

    #[test]
    fn test_process_rejects_invalid_requests() {
        let (worker, _source) = make_worker();
        let mut entry = UringEntry::new(0, worker.payload_cap);

        // Undefined opcodes are rejected.
        plant_request(&mut entry, 50, &[], &[]);
        assert!(worker.process(&mut entry).is_err());

        // A payload larger than the registered area is rejected.
        plant_request(
            &mut entry,
            Opcode::Getattr as u32,
            GetattrIn::default().as_slice(),
            &[],
        );
        entry.header_mut().ring_ent_in_out.payload_sz = (worker.payload_cap + 1) as u32;
        assert!(worker.process(&mut entry).is_err());
    }

    #[test]
    fn test_stage_error_reply_after_rejected_request() {
        let (worker, _source) = make_worker();
        let mut entry = UringEntry::new(0, worker.payload_cap);

        // Simulate the kernel delivering a request with an undefined
        // opcode: process() fails, and the worker must commit a proper
        // error reply rather than echoing the request data back.
        plant_request(&mut entry, 50, &[], &[]);
        assert!(worker.process(&mut entry).is_err());
        entry.stage_error_reply();

        let out = parse_out_header(&entry);
        assert_eq!(out.len, OUT_HEADER_SIZE as u32);
        assert_eq!(out.error, -libc::EIO);
        assert_eq!(out.unique, 2);
        assert_eq!(entry.header().ring_ent_in_out.payload_sz, 0);
        // The commit_id delivered by the kernel must be preserved so that
        // COMMIT_AND_FETCH matches the request.
        assert_eq!(entry.header().ring_ent_in_out.commit_id, 2);
    }

    #[test]
    fn test_process_rejects_oversized_reply() {
        let (worker, _source) = make_worker();
        // A payload area too small to hold the GETATTR reply body must be
        // rejected instead of panicking in the staging copy.
        let mut entry = UringEntry::new(0, 8);
        plant_request(
            &mut entry,
            Opcode::Getattr as u32,
            GetattrIn::default().as_slice(),
            &[],
        );
        assert!(worker.process(&mut entry).is_err());
    }
}
