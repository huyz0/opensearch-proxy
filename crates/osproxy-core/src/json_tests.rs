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
fn validate_accepts_well_formed_and_rejects_garbage() {
    assert!(validate(br#"{"a":[1,2,{"b":-3.5e2}],"c":"x"}"#).is_ok());
    assert!(validate(b"  true  ").is_ok());
    assert!(validate(b"{").is_err());
    assert!(validate(b"{\"a\":1,}").is_err());
    assert!(validate(b"1 2").is_err());
}
