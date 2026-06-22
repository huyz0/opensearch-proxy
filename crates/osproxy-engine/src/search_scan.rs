//! Resumable, byte-driven search-response transform — the streaming counterpart
//! of the buffered [`shape_hits`](crate::read::shape_hits).
//!
//! A search response is `{... "hits": {... "hits": [ <hit>, <hit>, ... ] ...}
//! ...}`. The proxy must strip each hit's injected tenancy fields, reset its
//! `_index`, drop `_routing`, and map its `_id` back to logical — and pass every
//! sibling (`took`, `_shards`, and especially the potentially huge
//! `aggregations`) through untouched. This scanner does that **without ever
//! buffering more than one hit**: it forwards bytes verbatim until it locates the
//! `hits.hits` array, frames each element, hands it to the audited per-hit
//! transform [`shape_hit`], and forwards everything after
//! the array verbatim again.
//!
//! It is a byte-level state machine (resumable across arbitrary chunk
//! boundaries) that tracks only JSON structure — string/escape state and
//! `{}[]` nesting — never building a `Value` for anything but a single framed
//! hit. The only isolation-relevant new code here is element *framing*; the
//! actual field strip is reused, not reimplemented. A property test pins the
//! whole-stream output to the buffered `shape_hits` oracle for every input and
//! every chunk split (see `search_scan_tests.rs`).
//
// JUSTIFY(file-length): one cohesive state machine — the phase/element/skip
// types and the per-byte transition handlers are a single unit whose
// correctness is argued (and fuzzed) as a whole; splitting the transitions from
// the state they mutate would scatter the isolation-critical framing invariant
// across files for no real separation.

use serde_json::Value;

use crate::read::{shape_hit, ReadShape};

/// The per-response transform context: where each hit's logical view comes from.
/// Mirrors the arguments [`shape_hits`](crate::read::shape_hits) carries.
pub(crate) struct HitShaper {
    /// The client's logical index name to present on each hit.
    pub logical_index: String,
    /// The resolved partition, for mapping physical ids back to logical.
    pub partition: String,
    /// Injected field names to strip and the id rule to invert.
    pub shape: ReadShape,
}

impl HitShaper {
    /// Transforms one framed hit (a complete JSON value) into the client's
    /// logical view, reusing the audited [`shape_hit`]. A non-value element
    /// cannot occur in well-formed upstream JSON; it is passed through so the
    /// scanner never panics on malformed input.
    fn transform(&self, raw: &[u8]) -> Vec<u8> {
        match serde_json::from_slice::<Value>(raw) {
            Ok(mut hit) => {
                shape_hit(&mut hit, &self.logical_index, &self.partition, &self.shape);
                serde_json::to_vec(&hit).unwrap_or_else(|_| raw.to_vec())
            }
            Err(_) => raw.to_vec(),
        }
    }
}

/// Where the scanner is in the response structure. All bytes are forwarded
/// verbatim except the framed hit elements, which are replaced by their
/// transformed form.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Skipping whitespace before the root object's `{`.
    SeekRoot,
    /// In an object (`level` 1 = root, 2 = the `hits` object), expecting a key
    /// `"` or the closing `}`.
    ObjExpectKey,
    /// Reading a member key string into `key_buf`.
    ObjReadKey,
    /// After a key, expecting `:`.
    ObjExpectColon,
    /// After `:`, expecting the value's first (non-whitespace) byte.
    ObjExpectValue,
    /// Forwarding a non-matching member value verbatim, tracking nesting.
    SkipValue,
    /// After a member value, expecting `,` or `}`.
    ObjExpectComma,
    /// In the `hits.hits` array, expecting an element or the closing `]`.
    ArrExpectElem,
    /// Buffering one array element.
    ArrReadElem,
    /// After an element, expecting `,` or `]`.
    ArrExpectComma,
    /// The array is handled (or no array was found): forward all remaining bytes.
    Passthrough,
}

/// Whether the current byte was consumed, or the phase changed and the same byte
/// must be re-dispatched under the new phase (e.g. a scalar's terminating
/// delimiter, which belongs to the enclosing frame, not the value).
enum Flow {
    Consume,
    Redo,
}

/// Resumable tracking for a value being **skipped** verbatim (a non-matching
/// object member): nesting depth, in-string state, and whether it is a bare
/// scalar (which ends at a delimiter rather than a closing token).
#[derive(Default)]
struct Skip {
    depth: u32,
    in_str: bool,
    esc: bool,
    scalar: bool,
}

impl Skip {
    /// Begins skipping a value from its first byte.
    fn begin(first: u8) -> Self {
        match first {
            b'"' => Self {
                in_str: true,
                ..Self::default()
            },
            b'{' | b'[' => Self {
                depth: 1,
                ..Self::default()
            },
            _ => Self {
                scalar: true,
                ..Self::default()
            },
        }
    }
}

/// The kind of the element currently being framed, set from its first byte.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ElemKind {
    Unknown,
    Str,
    Struct,
    Scalar,
}

/// The one array element being framed: its raw bytes plus the structural cursor.
struct Elem {
    buf: Vec<u8>,
    kind: ElemKind,
    depth: u32,
    in_str: bool,
    esc: bool,
}

impl Elem {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            kind: ElemKind::Unknown,
            depth: 0,
            in_str: false,
            esc: false,
        }
    }

    /// Resets to frame the next element, retaining the buffer's capacity.
    fn reset(&mut self) {
        self.buf.clear();
        self.kind = ElemKind::Unknown;
        self.depth = 0;
        self.in_str = false;
        self.esc = false;
    }
}

/// The streaming search-hits transformer. Feed it response bytes; it emits
/// transformed bytes, holding at most one hit plus a small structural cursor.
pub(crate) struct SearchHitsScanner {
    shaper: HitShaper,
    phase: Phase,
    /// Object nesting we care about: 1 = root object, 2 = the `hits` object.
    level: u8,
    /// Whether the key just read decoded to `"hits"`.
    key_is_hits: bool,
    /// Raw bytes of the key being read (escapes intact), decoded at the close.
    key_buf: Vec<u8>,
    /// Escape state while reading a key.
    key_esc: bool,
    skip: Skip,
    elem: Elem,
    /// Emitted-bytes accumulator, drained each [`feed`](Self::feed).
    out: Vec<u8>,
}

impl SearchHitsScanner {
    /// Creates a scanner that will shape hits with `shaper`.
    pub(crate) fn new(shaper: HitShaper) -> Self {
        Self {
            shaper,
            phase: Phase::SeekRoot,
            level: 0,
            key_is_hits: false,
            key_buf: Vec::new(),
            key_esc: false,
            skip: Skip::default(),
            elem: Elem::new(),
            out: Vec::new(),
        }
    }

    /// Feeds one chunk of response bytes, returning the transformed bytes ready
    /// to emit (possibly empty — e.g. a chunk consumed entirely into a partial
    /// element).
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Vec<u8> {
        for &b in chunk {
            self.step(b);
        }
        std::mem::take(&mut self.out)
    }

    /// Flushes at end of stream. For well-formed input the scanner has already
    /// emitted everything incrementally and ends with no pending element; a
    /// non-empty `elem` means truncated upstream JSON, which is dropped rather
    /// than emitted untransformed (it could be a partial, unstripped hit).
    pub(crate) fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.out)
    }

    /// Processes one byte, re-dispatching it across phases until it is consumed.
    fn step(&mut self, b: u8) {
        loop {
            let flow = match self.phase {
                Phase::SeekRoot => self.at_seek_root(b),
                Phase::ObjExpectKey => self.at_obj_expect_key(b),
                Phase::ObjReadKey => self.at_obj_read_key(b),
                Phase::ObjExpectColon => self.at_obj_expect_colon(b),
                Phase::ObjExpectValue => self.at_obj_expect_value(b),
                Phase::SkipValue => self.at_skip_value(b),
                Phase::ObjExpectComma => self.at_obj_expect_comma(b),
                Phase::ArrExpectElem => self.at_arr_expect_elem(b),
                Phase::ArrReadElem => self.read_elem(b),
                Phase::ArrExpectComma => self.at_arr_expect_comma(b),
                Phase::Passthrough => {
                    self.out.push(b);
                    Flow::Consume
                }
            };
            if matches!(flow, Flow::Consume) {
                return;
            }
        }
    }

    fn at_seek_root(&mut self, b: u8) -> Flow {
        self.out.push(b);
        if b == b'{' {
            self.level = 1;
            self.phase = Phase::ObjExpectKey;
        } else if !is_ws(b) {
            // Not an object: nothing to shape, forward the rest.
            self.phase = Phase::Passthrough;
        }
        Flow::Consume
    }

    fn at_obj_expect_key(&mut self, b: u8) -> Flow {
        self.out.push(b);
        if b == b'"' {
            self.key_buf.clear();
            self.key_esc = false;
            self.phase = Phase::ObjReadKey;
        } else if !is_ws(b) {
            // `}` (object closed without `hits`) or malformed: forward.
            self.phase = Phase::Passthrough;
        }
        Flow::Consume
    }

    fn at_obj_read_key(&mut self, b: u8) -> Flow {
        self.out.push(b);
        if self.key_esc {
            self.key_esc = false;
            self.key_buf.push(b);
        } else if b == b'\\' {
            self.key_esc = true;
            self.key_buf.push(b);
        } else if b == b'"' {
            self.key_is_hits = decoded_key_is_hits(&self.key_buf);
            self.phase = Phase::ObjExpectColon;
        } else {
            self.key_buf.push(b);
        }
        Flow::Consume
    }

    fn at_obj_expect_colon(&mut self, b: u8) -> Flow {
        self.out.push(b);
        if b == b':' {
            self.phase = Phase::ObjExpectValue;
        } else if !is_ws(b) {
            self.phase = Phase::Passthrough;
        }
        Flow::Consume
    }

    fn at_obj_expect_value(&mut self, b: u8) -> Flow {
        if is_ws(b) {
            self.out.push(b);
            return Flow::Consume;
        }
        self.out.push(b);
        if self.key_is_hits {
            // The root `hits` value must be an object; the inner `hits` value must
            // be an array. Anything else → no hits to shape (matching the buffered
            // path) → forward.
            match (self.level, b) {
                (1, b'{') => {
                    self.level = 2;
                    self.phase = Phase::ObjExpectKey;
                }
                (_, b'[') => self.phase = Phase::ArrExpectElem,
                _ => self.phase = Phase::Passthrough,
            }
        } else {
            self.skip = Skip::begin(b);
            self.phase = Phase::SkipValue;
        }
        Flow::Consume
    }

    fn at_skip_value(&mut self, b: u8) -> Flow {
        if self.skip.in_str {
            self.out.push(b);
            if self.skip.esc {
                self.skip.esc = false;
            } else if b == b'\\' {
                self.skip.esc = true;
            } else if b == b'"' {
                self.skip.in_str = false;
                if self.skip.depth == 0 {
                    self.phase = Phase::ObjExpectComma;
                }
            }
            return Flow::Consume;
        }
        if self.skip.depth > 0 {
            self.out.push(b);
            match b {
                b'"' => self.skip.in_str = true,
                b'{' | b'[' => self.skip.depth += 1,
                b'}' | b']' => {
                    self.skip.depth -= 1;
                    if self.skip.depth == 0 {
                        self.phase = Phase::ObjExpectComma;
                    }
                }
                _ => {}
            }
            return Flow::Consume;
        }
        // A bare scalar ends at a delimiter it must not consume.
        debug_assert!(self.skip.scalar);
        if is_ws(b) || b == b',' || b == b'}' {
            self.phase = Phase::ObjExpectComma;
            return Flow::Redo;
        }
        self.out.push(b);
        Flow::Consume
    }

    fn at_obj_expect_comma(&mut self, b: u8) -> Flow {
        self.out.push(b);
        if b == b',' {
            self.phase = Phase::ObjExpectKey;
        } else if !is_ws(b) {
            // `}` closes this object; anything after is forwarded.
            self.phase = Phase::Passthrough;
        }
        Flow::Consume
    }

    fn at_arr_expect_elem(&mut self, b: u8) -> Flow {
        if is_ws(b) {
            self.out.push(b);
            return Flow::Consume;
        }
        if b == b']' {
            // Empty array or end of elements.
            self.out.push(b);
            self.phase = Phase::Passthrough;
            return Flow::Consume;
        }
        // Element content: divert to `elem` (do not forward verbatim).
        self.elem.reset();
        self.phase = Phase::ArrReadElem;
        Flow::Redo
    }

    /// Frames one array element into `elem`. On completion, transforms it, emits
    /// the result, and advances to [`Phase::ArrExpectComma`] — re-dispatching a
    /// scalar's terminating delimiter, which belongs to the array frame.
    fn read_elem(&mut self, b: u8) -> Flow {
        match self.elem.kind {
            ElemKind::Unknown => {
                self.elem.buf.push(b);
                self.elem.kind = match b {
                    b'"' => {
                        self.elem.in_str = true;
                        ElemKind::Str
                    }
                    b'{' | b'[' => {
                        self.elem.depth = 1;
                        ElemKind::Struct
                    }
                    _ => ElemKind::Scalar,
                };
                Flow::Consume
            }
            ElemKind::Str => {
                self.elem.buf.push(b);
                if self.elem.esc {
                    self.elem.esc = false;
                } else if b == b'\\' {
                    self.elem.esc = true;
                } else if b == b'"' {
                    self.finish_elem();
                    self.phase = Phase::ArrExpectComma;
                }
                Flow::Consume
            }
            ElemKind::Struct => {
                self.elem.buf.push(b);
                self.read_struct_elem_byte(b);
                Flow::Consume
            }
            ElemKind::Scalar => {
                if is_ws(b) || b == b',' || b == b']' {
                    self.finish_elem();
                    self.phase = Phase::ArrExpectComma;
                    Flow::Redo
                } else {
                    self.elem.buf.push(b);
                    Flow::Consume
                }
            }
        }
    }

    /// Advances the structural cursor for one byte of an object/array element
    /// (already pushed to `elem.buf`); finishes the element when it closes.
    fn read_struct_elem_byte(&mut self, b: u8) {
        if self.elem.in_str {
            if self.elem.esc {
                self.elem.esc = false;
            } else if b == b'\\' {
                self.elem.esc = true;
            } else if b == b'"' {
                self.elem.in_str = false;
            }
            return;
        }
        match b {
            b'"' => self.elem.in_str = true,
            b'{' | b'[' => self.elem.depth += 1,
            b'}' | b']' => {
                self.elem.depth -= 1;
                if self.elem.depth == 0 {
                    self.finish_elem();
                    self.phase = Phase::ArrExpectComma;
                }
            }
            _ => {}
        }
    }

    fn at_arr_expect_comma(&mut self, b: u8) -> Flow {
        self.out.push(b);
        if b == b',' {
            self.phase = Phase::ArrExpectElem;
        } else if !is_ws(b) {
            // `]` ends the array; anything after is forwarded.
            self.phase = Phase::Passthrough;
        }
        Flow::Consume
    }

    /// Transforms the framed element and emits it.
    fn finish_elem(&mut self) {
        let shaped = self.shaper.transform(&self.elem.buf);
        self.out.extend_from_slice(&shaped);
        self.elem.buf.clear();
    }
}

/// Whether `b` is JSON insignificant whitespace.
fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Whether the raw key bytes (between the quotes, escapes intact) decode to
/// `hits`. Re-wraps them as a JSON string literal and lets serde decode (so a
/// spoofed `hits` is handled exactly as the buffered path's serde decode).
fn decoded_key_is_hits(raw: &[u8]) -> bool {
    let mut lit = Vec::with_capacity(raw.len() + 2);
    lit.push(b'"');
    lit.extend_from_slice(raw);
    lit.push(b'"');
    serde_json::from_slice::<String>(&lit).is_ok_and(|s| s == "hits")
}

#[cfg(test)]
#[path = "search_scan_tests.rs"]
mod tests;
