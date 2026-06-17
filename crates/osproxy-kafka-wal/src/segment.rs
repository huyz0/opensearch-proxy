//! The on-disk write-ahead log: an append-only `wal.log` of CRC-framed records
//! plus a `wal.ckpt` byte offset marking how far delivery has been acknowledged.
//!
//! One writer (the request thread, via `append`) and one reader (the drainer, via
//! `next`/`commit`) share a [`Wal`] behind a mutex. Appends are a positioned
//! `write` with no fsync, so the request path never blocks on the disk; the
//! drainer fsyncs the log periodically (group commit) and fsyncs the checkpoint on
//! every ack. A record is framed `[u32 len][u32 crc][len bytes]` so a partial tail
//! left by a hard crash fails the length/CRC check and is discarded on recovery.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// One record read back from the log: a queued produce plus the log offset just
/// past it, which [`Wal::commit`] persists once the record is acknowledged.
pub(crate) struct Record {
    pub topic: String,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub next: u64,
}

/// The append-only log and its acknowledgement checkpoint.
pub(crate) struct Wal {
    log: File,
    ckpt_path: PathBuf,
    /// End of the log: where the next append lands and how big the file is.
    write_offset: u64,
    /// Start of undelivered records: everything before is acknowledged.
    read_offset: u64,
    /// Reclaim the acknowledged prefix once it reaches this many bytes (so a
    /// steadily-draining buffer does not grow without bound).
    compact_threshold: u64,
    /// Hard cap on the live file: an append that would exceed it is refused.
    max_bytes: u64,
}

impl Wal {
    /// Opens (or creates) the log under `dir`, resuming the read cursor from the
    /// persisted checkpoint so undelivered records replay after a restart.
    pub(crate) fn open(dir: &Path, max_bytes: u64, compact_threshold: u64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("wal.log"))?;
        let write_offset = log.seek(SeekFrom::End(0))?;
        let ckpt_path = dir.join("wal.ckpt");
        let read_offset = read_ckpt(&ckpt_path).min(write_offset);
        Ok(Self {
            log,
            ckpt_path,
            write_offset,
            read_offset,
            compact_threshold,
            max_bytes,
        })
    }

    /// Appends one record. Returns `Err(())` when the live file would exceed the
    /// cap (the buffer is full; the caller drops the record to bound disk).
    pub(crate) fn append(&mut self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        let body = encode_body(topic, key, payload).ok_or(())?;
        let frame_len = 8 + body.len() as u64;
        if self.write_offset.saturating_add(frame_len) > self.max_bytes {
            return Err(());
        }
        self.write_at(self.write_offset, &body).map_err(|_| ())?;
        self.write_offset += frame_len;
        Ok(())
    }

    /// The next undelivered record, or `None` when the drainer has caught up. A
    /// corrupt or partial tail (a hard-crash artifact) is treated as the end and
    /// discarded.
    pub(crate) fn next(&mut self) -> Option<Record> {
        if self.read_offset >= self.write_offset {
            return None;
        }
        if let Some(record) = self.read_frame(self.read_offset) {
            return Some(record);
        }
        // Torn tail: drop everything from the read cursor onward.
        self.write_offset = self.read_offset;
        None
    }

    /// Advances the acknowledged cursor to `next` and fsyncs the checkpoint, so a
    /// restart does not redeliver this record (beyond the inherent at-least-once
    /// window between the broker ack and this write).
    pub(crate) fn commit(&mut self, next: u64) {
        self.read_offset = next;
        let _ = persist_ckpt(&self.ckpt_path, next);
    }

    /// Flushes the log to disk (group commit for appended-but-undelivered records).
    pub(crate) fn sync(&mut self) {
        let _ = self.log.sync_data();
    }

    /// Reclaims the acknowledged prefix by moving the live tail to the front, when
    /// the buffer is fully drained or the prefix has grown past the threshold.
    pub(crate) fn maybe_compact(&mut self) -> io::Result<()> {
        let caught_up = self.read_offset >= self.write_offset;
        if self.read_offset == 0 || (!caught_up && self.read_offset < self.compact_threshold) {
            return Ok(());
        }
        let live = self.write_offset - self.read_offset;
        if live > 0 {
            let mut buf = vec![0u8; usize::try_from(live).unwrap_or(usize::MAX)];
            self.log.seek(SeekFrom::Start(self.read_offset))?;
            self.log.read_exact(&mut buf)?;
            self.write_at(0, &buf)?;
        }
        self.log.set_len(live)?;
        self.log.sync_data()?;
        self.write_offset = live;
        self.read_offset = 0;
        persist_ckpt(&self.ckpt_path, 0)
    }

    fn write_at(&mut self, at: u64, body: &[u8]) -> io::Result<()> {
        let len = u32::try_from(body.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
        self.log.seek(SeekFrom::Start(at))?;
        // Frame: length + CRC header, then the body.
        self.log.write_all(&len.to_le_bytes())?;
        self.log.write_all(&crc32(body).to_le_bytes())?;
        self.log.write_all(body)
    }

    fn read_frame(&mut self, at: u64) -> Option<Record> {
        self.log.seek(SeekFrom::Start(at)).ok()?;
        let mut header = [0u8; 8];
        self.log.read_exact(&mut header).ok()?;
        let len = u32::from_le_bytes(header[0..4].try_into().ok()?) as usize;
        let crc = u32::from_le_bytes(header[4..8].try_into().ok()?);
        let mut body = vec![0u8; len];
        self.log.read_exact(&mut body).ok()?;
        if crc32(&body) != crc {
            return None;
        }
        let (topic, key, payload) = decode_body(&body)?;
        Some(Record {
            topic,
            key,
            payload,
            next: at + 8 + len as u64,
        })
    }
}

/// `[u16 topic_len][topic][u32 key_len][key][payload]`. `None` if a length field
/// would overflow (an absurdly long topic or key); the caller drops the record.
fn encode_body(topic: &str, key: &[u8], payload: &[u8]) -> Option<Vec<u8>> {
    let topic_len = u16::try_from(topic.len()).ok()?;
    let key_len = u32::try_from(key.len()).ok()?;
    let mut body = Vec::with_capacity(6 + topic.len() + key.len() + payload.len());
    body.extend_from_slice(&topic_len.to_le_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&key_len.to_le_bytes());
    body.extend_from_slice(key);
    body.extend_from_slice(payload);
    Some(body)
}

fn decode_body(body: &[u8]) -> Option<(String, Vec<u8>, Vec<u8>)> {
    let mut at = 0;
    let topic_len = u16::from_le_bytes(body.get(at..at + 2)?.try_into().ok()?) as usize;
    at += 2;
    let topic = std::str::from_utf8(body.get(at..at + topic_len)?)
        .ok()?
        .to_owned();
    at += topic_len;
    let key_len = u32::from_le_bytes(body.get(at..at + 4)?.try_into().ok()?) as usize;
    at += 4;
    let key = body.get(at..at + key_len)?.to_vec();
    at += key_len;
    let payload = body.get(at..)?.to_vec();
    Some((topic, key, payload))
}

/// IEEE 802.3 CRC-32, hand-rolled to avoid a dependency for a few bytes per frame.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Reads the persisted acknowledged offset, or 0 if absent/unreadable.
fn read_ckpt(path: &Path) -> u64 {
    let Ok(bytes) = std::fs::read(path) else {
        return 0;
    };
    bytes
        .get(0..8)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u64::from_le_bytes)
}

/// Persists the acknowledged offset durably (write + fsync).
fn persist_ckpt(path: &Path, offset: u64) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(&offset.to_le_bytes())?;
    file.sync_data()
}
