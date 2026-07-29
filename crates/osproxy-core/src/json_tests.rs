//! Unit tests for the byte-level JSON scanner ([`super`]).

use super::*;

#[test]
fn top_level_keys_and_insert_point() {
    let body = br#"{"a":1,"b":{"c":2},"d":"x"}"#;
    let top = object_top_level(body).unwrap();
    assert!(!top.empty);
    assert_eq!(top.keys, vec!["a", "b", "d"]);
    assert_eq!(top.insert_at, 1); // just past `{`
}

#[test]
fn empty_object_is_marked_empty() {
    let top = object_top_level(b"{}").unwrap();
    assert!(top.empty);
    assert!(top.keys.is_empty());
    assert_eq!(top.insert_at, 1);
}

#[test]
fn decodes_escaped_key_so_collision_cannot_be_smuggled() {
    // "_tenant" is "_tenant".
    let body = br#"{"_tenant":"evil"}"#;
    let top = object_top_level(body).unwrap();
    assert_eq!(top.keys, vec!["_tenant"]);
}

#[test]
fn non_object_and_malformed_are_distinguished() {
    assert_eq!(
        object_top_level(b"[1,2]").unwrap_err(),
        JsonError::NotAnObject
    );
    assert_eq!(
        object_top_level(b"  42 ").unwrap_err(),
        JsonError::NotAnObject
    );
    assert_eq!(
        object_top_level(b"not json").unwrap_err(),
        JsonError::Invalid
    );
    assert_eq!(
        object_top_level(b"{\"a\":}").unwrap_err(),
        JsonError::Invalid
    );
    assert_eq!(
        object_top_level(b"{} junk").unwrap_err(),
        JsonError::Invalid
    );
}

#[test]
fn scalar_at_path_reads_nested_and_typed_leaves() {
    let body = br#"{"meta":{"tenant":"acme"},"n":7,"flag":true}"#;
    assert_eq!(scalar_at_path(body, ["meta", "tenant"]).unwrap(), "acme");
    assert_eq!(scalar_at_path(body, ["n"]).unwrap(), "7");
    assert_eq!(scalar_at_path(body, ["flag"]).unwrap(), "true");
}

#[test]
fn scalar_at_path_rejects_missing_and_non_scalar() {
    let body = br#"{"a":{"b":[1,2]},"obj":{},"nil":null}"#;
    assert!(matches!(
        scalar_at_path(body, ["a", "b"]).unwrap_err(),
        JsonError::PathNotScalar { .. }
    ));
    assert!(scalar_at_path(body, ["obj"]).is_err());
    assert!(scalar_at_path(body, ["nil"]).is_err());
    assert!(scalar_at_path(body, ["missing"]).is_err());
}

#[test]
fn scalar_at_path_decodes_escaped_string_value() {
    let body = br#"{"k":"abc\n"}"#;
    assert_eq!(scalar_at_path(body, ["k"]).unwrap(), "abc\n");
}

#[test]
fn scalar_at_path_preserves_multibyte_utf8() {
    // A literal (un-escaped) multi-byte UTF-8 value must survive verbatim: a
    // non-ASCII partition key would otherwise be mojibake'd on ingest, breaking
    // write/read symmetry (the write injects the corrupted value, the read filters
    // on the verbatim one). Covers 2-byte (é), 3-byte (€), and 4-byte (😀).
    let body = "{\"tenant\":\"café-€-😀\"}".as_bytes();
    assert_eq!(scalar_at_path(body, ["tenant"]).unwrap(), "café-€-😀");
    // The same value reached via `\u` escapes (incl. a surrogate pair for 😀 =
    // U+1F600) decodes identically, so escaped and literal forms agree.
    let escaped = b"{\"tenant\":\"caf\\u00e9-\\u20ac-\\ud83d\\ude00\"}";
    assert_eq!(scalar_at_path(escaped, ["tenant"]).unwrap(), "café-€-😀");
}

#[test]
fn decoded_key_preserves_multibyte_utf8() {
    // Key decoding shares the same path; a non-ASCII key must not be corrupted.
    let body = "{\"naïve\":1}".as_bytes();
    let top = object_top_level(body).unwrap();
    assert_eq!(top.keys, vec!["naïve"]);
}

#[test]
fn validate_accepts_well_formed_and_rejects_garbage() {
    assert!(validate(br#"{"a":[1,2,{"b":-3.5e2}],"c":"x"}"#).is_ok());
    assert!(validate(b"  true  ").is_ok());
    assert!(validate(b"{").is_err());
    assert!(validate(b"{\"a\":1,}").is_err());
    assert!(validate(b"1 2").is_err());
}

#[test]
fn rejects_raw_control_byte_in_string() {
    // A literal (unescaped) control byte inside a string is invalid JSON; the
    // bulk-jump scan must still catch it even though it isn't a `"` or `\`.
    let body = b"{\"a\":\"x\x01y\"}";
    assert_eq!(validate(body).unwrap_err(), JsonError::Invalid);
    assert!(object_top_level(body).is_err());
}

#[test]
fn rejects_unterminated_string() {
    assert_eq!(validate(b"\"abc").unwrap_err(), JsonError::Invalid);
    assert_eq!(
        validate(br#"{"a":"unterminated"#).unwrap_err(),
        JsonError::Invalid
    );
}

#[test]
fn handles_string_spanning_multiple_scan_runs() {
    // Several escapes force the bulk-jump loop to run more than once, mixing
    // ordinary runs (bulk-copied) with escape decoding (byte-by-byte).
    let body = br#"{"a":"abc\ndef\tghi\\jkl\"mno"}"#;
    assert_eq!(
        scalar_at_path(body, ["a"]).unwrap(),
        "abc\ndef\tghi\\jkl\"mno"
    );
}

#[test]
fn validate_large_string_with_no_escapes() {
    // Exercises the memchr fast path over a run far larger than any SIMD lane
    // width, with no escapes and no control bytes.
    let mut body = br#"{"payload":""#.to_vec();
    body.extend(std::iter::repeat_n(b'x', 64 * 1024));
    body.extend(br#""}"#);
    assert!(validate(&body).is_ok());
}
