//! Body and query transforms.
//!
//! Pure, streaming transforms with no network or placement lookup: bulk NDJSON
//! demux (`docs/04` §3), partition-filter query wrapping and response field
//! stripping (`docs/04` §4), and doc-id construction. Held to the highest
//! coverage bar including branch coverage (`docs/09`). Lands in M2/M3.
