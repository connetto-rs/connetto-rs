use connetto_core::exchange::ManifestRow;
use connetto_core::schema::{Major, SchemaVersion};

use super::{MAX_MANIFEST_ROWS, Message, Transfer, captured_under, manifest};

fn major(number: u32) -> Major {
    Major::new(number).expect("a major")
}

fn version(byte: u8) -> SchemaVersion {
    SchemaVersion::from_hash(vec![byte; 32])
}

fn rows(count: usize) -> Vec<ManifestRow> {
    (1..=count)
        .map(|seq| ManifestRow {
            seq: u64::try_from(seq).expect("a test sequence"),
            major: major(2),
            origin: None,
            digest: [3; 32],
        })
        .collect()
}

/// The row counts of each manifest message and its last flag.
fn shape(count: usize) -> Vec<(usize, bool)> {
    manifest(7, [1; 32], rows(count))
        .map(|message| match message {
            Message::Transfer(Transfer::Manifest {
                request_id,
                previous,
                rows,
                last,
            }) => {
                assert_eq!((request_id, previous), (7, [1; 32]));
                (rows.len(), last)
            }
            other => panic!("a manifest yields manifests, got {}", other.named()),
        })
        .collect()
}

/// A range is cut at the row bound, its last message marked, and an empty
/// range is still one message so the receiver learns the range is empty.
#[test]
fn a_manifest_is_cut_at_the_row_bound_and_marks_its_last_message() {
    assert_eq!(shape(0), [(0, true)]);
    assert_eq!(shape(3), [(3, true)]);
    assert_eq!(shape(MAX_MANIFEST_ROWS), [(MAX_MANIFEST_ROWS, true)]);
    assert_eq!(
        shape(MAX_MANIFEST_ROWS + 1),
        [(MAX_MANIFEST_ROWS, false), (1, true)]
    );
    let rows: Vec<u64> = manifest(7, [1; 32], rows(2 * MAX_MANIFEST_ROWS + 5))
        .flat_map(|message| match message {
            Message::Transfer(Transfer::Manifest { rows, .. }) => {
                rows.into_iter().map(|row| row.seq)
            }
            other => panic!("a manifest yields manifests, got {}", other.named()),
        })
        .collect();
    assert_eq!(
        rows,
        (1..=u64::try_from(2 * MAX_MANIFEST_ROWS + 5).expect("fits")).collect::<Vec<_>>(),
        "every row crosses once, in order"
    );
}

/// Every message `manifest` yields keeps the bound, and one row over it is
/// refused.
#[test]
fn the_row_bound_refuses_one_row_over() {
    assert!(
        manifest(7, [1; 32], rows(MAX_MANIFEST_ROWS + 1))
            .all(|message| message.within_bounds().is_ok())
    );
    let over = Message::Transfer(Transfer::Manifest {
        request_id: 7,
        previous: [1; 32],
        rows: rows(MAX_MANIFEST_ROWS + 1),
        last: true,
    });
    assert_eq!(over.within_bounds(), Err(super::BoundsError::TooManyRows));
}

/// An entry's body crosses only when the author's version of its major is
/// the requester's build (decision 8).
#[test]
fn a_body_crosses_only_under_the_requesters_build() {
    let builds = [(major(1), version(1)), (major(2), version(2))];
    assert!(captured_under(&builds, major(2), &(major(2), version(2))));
    assert!(
        !captured_under(&builds, major(1), &(major(2), version(2))),
        "another major crosses as its row"
    );
    assert!(
        !captured_under(&builds, major(2), &(major(2), version(9))),
        "the same major under another version crosses as its row"
    );
    assert!(
        !captured_under(&builds[..1], major(2), &(major(2), version(2))),
        "a major the author does not name crosses as its row"
    );
}

/// A body crosses as `MessagePack` binary, not as an array of integers.
#[test]
fn a_body_encodes_its_changeset_as_binary() {
    let body = Message::Transfer(Transfer::Body {
        request_id: 1,
        seq: 1,
        changeset: vec![0xff; 1024],
    });
    let bytes = rmp_serde::to_vec(&body).expect("encodes");
    assert!(
        bytes.len() < 1024 + 32,
        "1 KiB of changeset took {} bytes",
        bytes.len()
    );
    assert_eq!(
        rmp_serde::from_slice::<Message>(&bytes).expect("decodes"),
        body
    );
}
