//! Vectorized non-blank-line splitting shared by the buffered NDJSON body
//! parsers ([`crate::parse_bulk`], [`crate::parse_msearch`]).

/// Splits `body` on `\n`, skipping blank (all-whitespace) lines — the framing
/// both `_bulk` and `_msearch` bodies share.
///
/// Uses a vectorized (`memchr`) newline scan instead of `slice::split`'s
/// per-byte predicate closure, so splitting a body with many lines (a large
/// bulk request) stays fast.
pub(crate) fn non_blank_lines(body: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    std::iter::from_fn(move || {
        while start <= body.len() {
            let end = memchr::memchr(b'\n', &body[start..]).map_or(body.len(), |rel| start + rel);
            let line = &body[start..end];
            start = end + 1;
            if !line.iter().all(u8::is_ascii_whitespace) {
                return Some(line);
            }
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_split_by_newline_filtering_blank_lines() {
        let cases: &[&[u8]] = &[
            b"",
            b"a",
            b"a\n",
            b"a\nb\n",
            b"\n\n",
            b"a\n\nb",
            b"  \n a \n\n",
            b"\n",
            b"   ",
        ];
        for body in cases {
            let via_split: Vec<&[u8]> = body
                .split(|&b| b == b'\n')
                .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
                .collect();
            let via_memchr: Vec<&[u8]> = non_blank_lines(body).collect();
            assert_eq!(via_memchr, via_split, "body={body:?}");
        }
    }
}
