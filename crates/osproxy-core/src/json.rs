//! Byte-level JSON scanning for the no-materialization body path (ADR-014).
//!
//! These routines read exactly what a tenancy transform needs — the set of
//! top-level field names (to detect a spoofed reserved field) and a scalar at a
//! path (to find the partition key or build an id) — by scanning the raw body
//! bytes, **without ever building a parsed JSON tree**. Retained memory is
//! bounded by the few small key strings (or the one extracted scalar), never by
//! document size (INV-MEM): every value the scan does not need is skipped without
//! allocating.
//!
//! It lives in `core` because it is dependency-free pure computation that both
//! the SPI (partition extraction utilities) and the transform layer (id
//! construction, field-splice injection) build on — the two sides cannot share a
//! helper that lives in either of them.
//!
//! The scanner is strict: it parses the JSON grammar fully so a malformed body
//! is rejected here rather than mis-located. Key strings are decoded before they
//! are compared, so a client cannot smuggle a reserved field name past a
//! collision check by escaping it (e.g. `"_tenant"` for `_tenant`).
//
// JUSTIFY(file-length): one cohesive recursive-descent JSON scanner — the
// `Parser` and its grammar productions (value/object/array/string/number/escape)
// are a single unit that must agree on cursor invariants; splitting the
// productions across files would scatter that shared state for no readability
// gain. Tests live separately in `json_tests.rs`.

use thiserror::Error;

/// A failure scanning raw JSON bytes.
///
/// Deliberately exhaustive (not `#[non_exhaustive]`): it is a small, closed set
/// of JSON-shape failures, and downstream `From` conversions must map every
/// variant — a new one should be a compile error to handle, not silently fall
/// through a wildcard.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum JsonError {
    /// The bytes were not valid JSON.
    #[error("not valid JSON")]
    Invalid,

    /// The document was expected to be a JSON object but was not.
    #[error("not a JSON object")]
    NotAnObject,

    /// A path does not resolve to a scalar value in the document.
    #[error("path does not resolve to a scalar value")]
    PathNotScalar {
        /// The dotted path that failed to resolve.
        path: String,
    },
}

/// The located top level of a JSON object: where to splice injected fields,
/// whether it already has members, and its decoded top-level key names.
#[derive(Debug)]
pub struct TopLevel {
    /// Byte offset just past the opening `{` — the splice insertion point.
    pub insert_at: usize,
    /// True if the object has no members (`{}`) — no trailing comma on splice.
    pub empty: bool,
    /// Decoded top-level key names (escapes resolved), for collision checks.
    pub keys: Vec<String>,
}

/// Locates the top level of the JSON object in `body`, validating the whole
/// document as it goes.
///
/// # Errors
///
/// [`JsonError::NotAnObject`] if `body` is valid JSON but not an object,
/// [`JsonError::Invalid`] if it is not valid JSON.
pub fn object_top_level(body: &[u8]) -> Result<TopLevel, JsonError> {
    let mut p = Parser::new(body);
    p.skip_ws();
    if p.peek() != Some(b'{') {
        // Not an object: distinguish malformed JSON from a non-object value so
        // the caller can report the right error.
        return Err(match validate(body) {
            Ok(()) => JsonError::NotAnObject,
            Err(e) => e,
        });
    }
    let top = p.object_members()?;
    p.skip_ws();
    if p.peek().is_some() {
        return Err(JsonError::Invalid);
    }
    Ok(top)
}

/// Follows `segments` into the object in `body` and returns the leaf scalar as a
/// string: strings are decoded, numbers and bools use their source text.
///
/// # Errors
///
/// [`JsonError::PathNotScalar`] if a segment is missing or the leaf is an
/// object, array, or null; [`JsonError::Invalid`] if `body` up to the leaf is
/// not valid JSON.
pub fn scalar_at_path<'a, I>(body: &[u8], segments: I) -> Result<String, JsonError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut p = Parser::new(body);
    let mut walked: Vec<&str> = Vec::new();
    for segment in segments {
        walked.push(segment);
        p.enter_field(segment)
            .ok_or_else(|| JsonError::PathNotScalar {
                path: walked.join("."),
            })?;
    }
    p.skip_ws();
    p.scalar_string().ok_or_else(|| JsonError::PathNotScalar {
        path: walked.join("."),
    })
}

/// Validates that `body` is a single well-formed JSON document (trailing
/// whitespace allowed), allocating nothing.
///
/// # Errors
///
/// [`JsonError::Invalid`] if `body` is not valid JSON.
pub fn validate(body: &[u8]) -> Result<(), JsonError> {
    let mut p = Parser::new(body);
    p.skip_value()?;
    p.skip_ws();
    if p.peek().is_some() {
        return Err(JsonError::Invalid);
    }
    Ok(())
}

/// A cursor over the raw JSON bytes.
struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    /// Parses the object at the cursor (which must be `{`), recording its top
    /// level. Used for the document root by [`object_top_level`].
    fn object_members(&mut self) -> Result<TopLevel, JsonError> {
        debug_assert_eq!(self.peek(), Some(b'{'));
        self.i += 1; // opening brace
        let insert_at = self.i;
        let mut keys = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(TopLevel {
                insert_at,
                empty: true,
                keys,
            });
        }
        loop {
            self.skip_ws();
            keys.push(self.string_decode()?);
            self.skip_ws();
            self.expect(b':')?;
            self.skip_value()?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(JsonError::Invalid),
            }
        }
        Ok(TopLevel {
            insert_at,
            empty: false,
            keys,
        })
    }

    /// Positions the cursor at the value of object member `key`, returning
    /// `Some(())` if found. On a miss (or a non-object), returns `None` and the
    /// cursor position is unspecified.
    fn enter_field(&mut self, key: &str) -> Option<()> {
        self.skip_ws();
        if self.peek() != Some(b'{') {
            return None;
        }
        self.i += 1;
        self.skip_ws();
        if self.peek() == Some(b'}') {
            return None;
        }
        loop {
            self.skip_ws();
            let k = self.string_decode().ok()?;
            self.skip_ws();
            self.expect(b':').ok()?;
            self.skip_ws();
            if k == key {
                return Some(());
            }
            self.skip_value().ok()?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                _ => return None,
            }
        }
    }

    /// Reads the scalar at the cursor as a string: strings decoded, numbers and
    /// bools as their source text. `None` for object/array/null/malformed.
    fn scalar_string(&mut self) -> Option<String> {
        match self.peek()? {
            b'"' => self.string_decode().ok(),
            b't' => self.literal(b"true").ok().map(|()| "true".to_owned()),
            b'f' => self.literal(b"false").ok().map(|()| "false".to_owned()),
            c if c == b'-' || c.is_ascii_digit() => {
                let start = self.i;
                self.number().ok()?;
                std::str::from_utf8(&self.b[start..self.i])
                    .ok()
                    .map(str::to_owned)
            }
            _ => None,
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), JsonError> {
        if self.peek() == Some(byte) {
            self.i += 1;
            Ok(())
        } else {
            Err(JsonError::Invalid)
        }
    }

    /// Skips one complete JSON value, allocating nothing.
    fn skip_value(&mut self) -> Result<(), JsonError> {
        self.skip_ws();
        match self.peek().ok_or(JsonError::Invalid)? {
            b'{' => self.skip_object(),
            b'[' => self.skip_array(),
            b'"' => self.skip_string(),
            b't' => self.literal(b"true"),
            b'f' => self.literal(b"false"),
            b'n' => self.literal(b"null"),
            c if c == b'-' || c.is_ascii_digit() => self.number(),
            _ => Err(JsonError::Invalid),
        }
    }

    fn skip_object(&mut self) -> Result<(), JsonError> {
        self.i += 1; // '{'
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(());
        }
        loop {
            self.skip_ws();
            self.skip_string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_value()?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    return Ok(());
                }
                _ => return Err(JsonError::Invalid),
            }
        }
    }

    fn skip_array(&mut self) -> Result<(), JsonError> {
        self.i += 1; // '['
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(());
        }
        loop {
            self.skip_value()?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    return Ok(());
                }
                _ => return Err(JsonError::Invalid),
            }
        }
    }

    /// Skips a string (cursor at the opening quote), handling escapes, no alloc.
    fn skip_string(&mut self) -> Result<(), JsonError> {
        self.expect(b'"')?;
        loop {
            match self.peek().ok_or(JsonError::Invalid)? {
                b'"' => {
                    self.i += 1;
                    return Ok(());
                }
                b'\\' => {
                    self.i += 1;
                    // Consume the escaped char; `\u` carries four more hex digits.
                    let esc = self.peek().ok_or(JsonError::Invalid)?;
                    self.i += 1;
                    if esc == b'u' {
                        for _ in 0..4 {
                            self.hex_digit()?;
                        }
                    }
                }
                c if c < 0x20 => return Err(JsonError::Invalid),
                _ => self.i += 1,
            }
        }
    }

    /// Decodes a string (cursor at the opening quote) into an owned `String`.
    fn string_decode(&mut self) -> Result<String, JsonError> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.peek().ok_or(JsonError::Invalid)? {
                b'"' => {
                    self.i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.i += 1;
                    self.decode_escape(&mut out)?;
                }
                c if c < 0x20 => return Err(JsonError::Invalid),
                _ => {
                    // Copy one UTF-8 code unit; multi-byte sequences copy byte by
                    // byte (each continuation byte is >= 0x80, so it is not an
                    // escape or terminator and falls through here).
                    out.push(char::from(self.b[self.i]));
                    self.i += 1;
                }
            }
        }
    }

    /// Decodes one escape sequence (cursor just past the backslash) into `out`.
    fn decode_escape(&mut self, out: &mut String) -> Result<(), JsonError> {
        let esc = self.peek().ok_or(JsonError::Invalid)?;
        self.i += 1;
        let ch = match esc {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{0008}',
            b'f' => '\u{000C}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => return self.decode_unicode_escape(out),
            _ => return Err(JsonError::Invalid),
        };
        out.push(ch);
        Ok(())
    }

    /// Decodes a `\u` escape (cursor just past the `u`), pairing surrogates.
    fn decode_unicode_escape(&mut self, out: &mut String) -> Result<(), JsonError> {
        let hi = self.hex4()?;
        let code = if (0xD800..=0xDBFF).contains(&hi) {
            // High surrogate: must be followed by `\u` + a low surrogate.
            self.expect(b'\\')?;
            self.expect(b'u')?;
            let lo = self.hex4()?;
            if !(0xDC00..=0xDFFF).contains(&lo) {
                return Err(JsonError::Invalid);
            }
            0x1_0000 + ((u32::from(hi) - 0xD800) << 10) + (u32::from(lo) - 0xDC00)
        } else if (0xDC00..=0xDFFF).contains(&hi) {
            return Err(JsonError::Invalid); // lone low surrogate
        } else {
            u32::from(hi)
        };
        out.push(char::from_u32(code).ok_or(JsonError::Invalid)?);
        Ok(())
    }

    /// Reads four hex digits as a `u16` (cursor at the first digit).
    fn hex4(&mut self) -> Result<u16, JsonError> {
        let mut v: u16 = 0;
        for _ in 0..4 {
            let d = self.hex_digit()?;
            v = v * 16 + u16::from(d);
        }
        Ok(v)
    }

    /// Consumes one hex digit, returning its value.
    fn hex_digit(&mut self) -> Result<u8, JsonError> {
        let c = self.peek().ok_or(JsonError::Invalid)?;
        let v = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => return Err(JsonError::Invalid),
        };
        self.i += 1;
        Ok(v)
    }

    /// Validates and skips a JSON number (cursor at `-` or a digit).
    fn number(&mut self) -> Result<(), JsonError> {
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.peek() {
            Some(b'0') => self.i += 1,
            Some(c) if c.is_ascii_digit() => self.digits(),
            _ => return Err(JsonError::Invalid),
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            self.one_or_more_digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            self.one_or_more_digits()?;
        }
        Ok(())
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.i += 1;
        }
    }

    fn one_or_more_digits(&mut self) -> Result<(), JsonError> {
        if !matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            return Err(JsonError::Invalid);
        }
        self.digits();
        Ok(())
    }

    /// Matches an exact literal (`true`/`false`/`null`) at the cursor.
    fn literal(&mut self, lit: &[u8]) -> Result<(), JsonError> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(JsonError::Invalid)
        }
    }
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;
