//! The streaming output [`Sink`].
//!
//! A `Sink` is *not* an in-memory buffer of the whole response. It accumulates
//! bytes into a small buffer and, every ~4Kb, ships an owned `Vec<u8>` chunk
//! over a bounded channel to the HTTP layer, which flushes each chunk straight
//! to the response. The full response is never materialised.
//!
//! ## Producer / consumer split
//!
//! ```text
//!     rayon COMPUTE_POOL thread                 tokio / poem
//!   ┌──────────────────────────┐   chunk   ┌────────────────────────┐
//!   │ exec ─► Sink::write_*     │ ───────►  │ Body::from_bytes_stream │
//!   │  buf (Vec<u8>, cap ~4Kb)  │  channel  │  flush each Vec<u8>     │
//!   │  on ≥4Kb: send, reset     │           │  (no concatenation)     │
//!   │  on finish: flush + drop  │           │                         │
//!   └──────────────────────────┘           └────────────────────────┘
//! ```
//!
//! Execution is **spawned** on the compute pool, never awaited before the body
//! is returned: awaiting completion first would let the bounded channel fill
//! while nothing drains it, deadlocking the producer on `blocking_send`.

use crate::rayon::COMPUTE_POOL;
use poem::Body;
use std::io;
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Bytes are flushed to the channel once the buffer reaches this size.
const CHUNK_SIZE: usize = 4096;

/// Number of chunks that may be in flight before the producer blocks.
/// Bounds in-flight memory to roughly `CHANNEL_CAPACITY * CHUNK_SIZE`.
const CHANNEL_CAPACITY: usize = 8;

/// One open JSON container, tracking how many elements/members it already holds
/// so we know when to emit a separating comma.
#[derive(Clone, Copy)]
enum Frame {
    Object { count: usize },
    Array { count: usize },
}

/// Streaming JSON writer that emits well-formed GraphQL response JSON and ships
/// it to the HTTP layer in fixed-size chunks.
///
/// Callers use the typed helpers ([`begin_object`](Self::begin_object),
/// [`write_i64`](Self::write_i64), …) which track container/comma state so no
/// punctuation is hand-written. [`io::Write`] is also implemented for
/// `write!`-style use, but the typed methods are the primary surface.
pub struct Sink {
    buf: Vec<u8>,
    tx: Sender<Vec<u8>>,
    /// Stack of open containers (objects/arrays).
    frames: Vec<Frame>,
    /// True between [`begin_field`](Self::begin_field) and the value that
    /// follows it — so that value is emitted as the field's value rather than
    /// being treated as a new array/object element (no leading comma).
    pending_field: bool,
    /// Set once the consumer has gone away (client disconnected). Further writes
    /// become no-ops — the stream is effectively aborted.
    closed: bool,
}

impl Sink {
    fn new(tx: Sender<Vec<u8>>) -> Self {
        Self {
            buf: Vec::with_capacity(CHUNK_SIZE),
            tx,
            frames: Vec::new(),
            pending_field: false,
            closed: false,
        }
    }

    // ── byte plumbing ──────────────────────────────────────────────────────

    /// Append raw bytes, flushing a chunk if the buffer is full enough.
    fn put(&mut self, bytes: &[u8]) {
        if self.closed {
            return;
        }
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= CHUNK_SIZE {
            self.ship();
        }
    }

    fn put_byte(&mut self, b: u8) {
        if self.closed {
            return;
        }
        self.buf.push(b);
        if self.buf.len() >= CHUNK_SIZE {
            self.ship();
        }
    }

    /// Hand the current buffer to the channel and start a fresh one.
    ///
    /// Uses `blocking_send`: on a rayon worker (the only place this runs) that
    /// blocks the worker under backpressure, which is exactly what we want. A
    /// send error means the consumer dropped (client gone) — we mark the sink
    /// closed and silently stop producing.
    fn ship(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK_SIZE));
        if self.tx.blocking_send(chunk).is_err() {
            self.closed = true;
        }
    }

    // ── structure / separators ─────────────────────────────────────────────

    /// Emit a separating comma if needed before writing the next *value*.
    /// A value is either an array element or the value part of an object member
    /// (the latter signalled by `pending_field`, which suppresses the comma).
    fn before_value(&mut self) {
        if self.pending_field {
            self.pending_field = false;
            return;
        }
        // Object values only arrive via `begin_field`, which sets
        // `pending_field`; a bare value at the top level is the whole doc.
        if let Some(Frame::Array { count }) = self.frames.last_mut() {
            let need_comma = *count > 0;
            *count += 1; // last use of the frame borrow — released before put_byte
            if need_comma {
                self.put_byte(b',');
            }
        }
    }

    /// Open an object `{`. Valid as a top-level value, an array element, or a
    /// field value.
    pub fn begin_object(&mut self) {
        self.before_value();
        self.put_byte(b'{');
        self.frames.push(Frame::Object { count: 0 });
    }

    /// Close an object `}`.
    pub fn end_object(&mut self) {
        debug_assert!(matches!(self.frames.last(), Some(Frame::Object { .. })));
        self.frames.pop();
        self.put_byte(b'}');
    }

    /// Open an array `[`.
    pub fn begin_array(&mut self) {
        self.before_value();
        self.put_byte(b'[');
        self.frames.push(Frame::Array { count: 0 });
    }

    /// Close an array `]`.
    pub fn end_array(&mut self) {
        debug_assert!(matches!(self.frames.last(), Some(Frame::Array { .. })));
        self.frames.pop();
        self.put_byte(b']');
    }

    /// Begin an object member: emits `"key":` (with a leading comma if this is
    /// not the first member). The following value call fills it in.
    pub fn begin_field(&mut self, key: &str) {
        if let Some(Frame::Object { count }) = self.frames.last_mut() {
            let need_comma = *count > 0;
            *count += 1; // last use of the frame borrow — released before put_byte
            if need_comma {
                self.put_byte(b',');
            }
        }
        self.write_json_string(key);
        self.put_byte(b':');
        self.pending_field = true;
    }

    /// Convenience: a `"key": null` member.
    pub fn field_null(&mut self, key: &str) {
        self.begin_field(key);
        self.write_null();
    }

    // ── scalar values ────────────────────────────────────────────────────────

    /// Write a signed integer value.
    pub fn write_i64(&mut self, v: i64) {
        self.before_value();
        let mut buf = itoa_buf();
        let s = i64_to_str(v, &mut buf);
        self.put(s.as_bytes());
    }

    /// Write an unsigned integer value.
    pub fn write_u64(&mut self, v: u64) {
        self.before_value();
        let mut buf = itoa_buf();
        let s = u64_to_str(v, &mut buf);
        self.put(s.as_bytes());
    }

    /// Write a string value (JSON-escaped and quoted).
    pub fn write_str(&mut self, s: &str) {
        self.before_value();
        self.write_json_string(s);
    }

    /// Write a boolean value.
    pub fn write_bool(&mut self, b: bool) {
        self.before_value();
        self.put(if b { b"true" } else { b"false" });
    }

    /// Write a JSON `null` value.
    pub fn write_null(&mut self) {
        self.before_value();
        self.put(b"null");
    }

    /// Write a JSON-escaped, double-quoted string (used for both keys and
    /// string values).
    fn write_json_string(&mut self, s: &str) {
        self.put_byte(b'"');
        let bytes = s.as_bytes();
        let mut start = 0;
        for (i, &b) in bytes.iter().enumerate() {
            let escape: &[u8] = match b {
                b'"' => b"\\\"",
                b'\\' => b"\\\\",
                b'\n' => b"\\n",
                b'\r' => b"\\r",
                b'\t' => b"\\t",
                0x08 => b"\\b",
                0x0c => b"\\f",
                b if b < 0x20 => {
                    // other control chars → \u00XX
                    if start < i {
                        self.put(&bytes[start..i]);
                    }
                    let mut esc = [b'\\', b'u', b'0', b'0', 0, 0];
                    esc[4] = hex_digit(b >> 4);
                    esc[5] = hex_digit(b & 0xf);
                    self.put(&esc);
                    start = i + 1;
                    continue;
                }
                _ => continue,
            };
            if start < i {
                self.put(&bytes[start..i]);
            }
            self.put(escape);
            start = i + 1;
        }
        if start < bytes.len() {
            self.put(&bytes[start..]);
        }
        self.put_byte(b'"');
    }

    /// Flush the final partial chunk and close the channel. Always call this
    /// when production is complete so the consumer sees end-of-stream.
    pub fn finish(mut self) {
        self.ship();
        // dropping `self` (and thus `tx`) closes the channel.
    }
}

impl io::Write for Sink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.put(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.ship();
        Ok(())
    }
}

/// Spawn `producer` on the compute pool and return the consumer end of the
/// chunk channel. The producer builds a [`Sink`] over the sender, writes the
/// whole document, and finishes (closing the channel).
fn spawn_producer<F>(producer: F) -> Receiver<Vec<u8>>
where
    F: FnOnce(&mut Sink) + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAPACITY);
    COMPUTE_POOL.spawn_fifo(move || {
        let mut sink = Sink::new(tx);
        producer(&mut sink);
        sink.finish();
    });
    rx
}

/// Build a streaming poem [`Body`] fed by `producer`, which runs on the compute
/// pool and writes the response into the [`Sink`] it is given. The body yields
/// each ~4Kb chunk as it is produced; nothing is concatenated.
pub fn streaming_body<F>(producer: F) -> Body
where
    F: FnOnce(&mut Sink) + Send + 'static,
{
    let rx = spawn_producer(producer);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk| (Ok::<Vec<u8>, io::Error>(chunk), rx))
    });
    Body::from_bytes_stream(stream)
}

// ── small integer formatting helpers (no external itoa dep) ──────────────────

type ItoaBuf = [u8; 20];

fn itoa_buf() -> ItoaBuf {
    [0u8; 20]
}

fn u64_to_str(mut v: u64, buf: &mut ItoaBuf) -> &str {
    if v == 0 {
        return "0";
    }
    let mut i = buf.len();
    while v > 0 {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    // SAFETY: only ASCII digits were written.
    std::str::from_utf8(&buf[i..]).unwrap()
}

fn i64_to_str(v: i64, buf: &mut ItoaBuf) -> String {
    if v < 0 {
        // unsigned_abs handles i64::MIN correctly
        let mut s = String::with_capacity(20);
        s.push('-');
        s.push_str(u64_to_str(v.unsigned_abs(), buf));
        s
    } else {
        u64_to_str(v as u64, buf).to_string()
    }
}

fn hex_digit(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        _ => b'a' + (nibble - 10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// Drain the producer's chunks and reassemble the full document (collecting
    /// here is a *test-only* convenience — the engine itself never concatenates).
    async fn collect(producer: impl FnOnce(&mut Sink) + Send + 'static) -> Vec<u8> {
        let mut rx = spawn_producer(producer);
        let mut out = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk);
        }
        out
    }

    async fn collect_json(producer: impl FnOnce(&mut Sink) + Send + 'static) -> Value {
        let bytes = collect(producer).await;
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("not valid JSON: {e}\n{}", String::from_utf8_lossy(&bytes)))
    }

    #[tokio::test]
    async fn nested_object_and_array() {
        let v = collect_json(|s| {
            s.begin_object();
            s.begin_field("data");
            s.begin_object();
            s.begin_field("hello");
            s.write_str("world");
            s.begin_field("nums");
            s.begin_array();
            for i in 0..3 {
                s.write_i64(i);
            }
            s.end_array();
            s.begin_field("missing");
            s.write_null();
            s.end_object();
            s.end_object();
        })
        .await;
        assert_eq!(
            v,
            json!({"data": {"hello": "world", "nums": [0, 1, 2], "missing": null}})
        );
    }

    #[tokio::test]
    async fn array_of_objects_like_history() {
        // mirrors graph -> node -> history -> list -> {timestamp, eventId}
        let v = collect_json(|s| {
            s.begin_object();
            s.begin_field("list");
            s.begin_array();
            for (ts, id) in [(555i64, 93u64), (562, 104)] {
                s.begin_object();
                s.begin_field("timestamp");
                s.write_i64(ts);
                s.begin_field("eventId");
                s.write_u64(id);
                s.end_object();
            }
            s.end_array();
            s.end_object();
        })
        .await;
        assert_eq!(
            v,
            json!({"list": [
                {"timestamp": 555, "eventId": 93},
                {"timestamp": 562, "eventId": 104},
            ]})
        );
    }

    #[tokio::test]
    async fn string_escaping() {
        let v = collect_json(|s| {
            s.begin_object();
            s.begin_field("k\"odd");
            s.write_str("a\"b\\c\nd\te");
            s.end_object();
        })
        .await;
        assert_eq!(v, json!({"k\"odd": "a\"b\\c\nd\te"}));
    }

    #[tokio::test]
    async fn spans_multiple_chunks() {
        // emit far more than one chunk to exercise the channel + chunking path
        let n = 5000i64;
        let bytes = collect(move |s| {
            s.begin_object();
            s.begin_field("nums");
            s.begin_array();
            for i in 0..n {
                s.write_i64(i);
            }
            s.end_array();
            s.end_object();
        })
        .await;
        assert!(
            bytes.len() > CHUNK_SIZE * 2,
            "expected multiple chunks, got {} bytes",
            bytes.len()
        );
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        let nums = v["nums"].as_array().unwrap();
        assert_eq!(nums.len(), n as usize);
        assert_eq!(nums[0], json!(0));
        assert_eq!(nums[(n - 1) as usize], json!(n - 1));
    }

    #[tokio::test]
    async fn poc_route_streams_over_http() {
        use crate::GraphServer;
        use raphtory::db::api::storage::storage::Config;
        use std::time::Duration;
        use tempfile::TempDir;

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default())
            .await
            .unwrap();
        let port = 43931;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await; // wait for the server to be up

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://localhost:{port}/graphql_stream_poc"))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        // streamed, not buffered: chunked transfer-encoding and no fixed length
        assert!(resp.headers().get("content-length").is_none());

        let body = resp.text().await.unwrap();
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["data"]["hello"],
            json!("world from the streaming interpreter")
        );
        let nums = v["data"]["nums"].as_array().unwrap();
        assert_eq!(nums.len(), 2000);
        assert_eq!(nums[0], json!(0));
        assert_eq!(nums[1999], json!(1999));
    }

    #[test]
    fn int_formatting() {
        let mut b = itoa_buf();
        assert_eq!(u64_to_str(0, &mut b), "0");
        assert_eq!(u64_to_str(12345, &mut b), "12345");
        assert_eq!(i64_to_str(-42, &mut b), "-42");
        assert_eq!(i64_to_str(i64::MIN, &mut b), i64::MIN.to_string());
    }
}
