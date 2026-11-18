//! The keyed diff a live subscription crosses a process boundary with (R116).

use connetto_client::ClientError;
use connetto_client::live_wire::{KeyedDiff, KeyedRow, decode, encode, rows_array};
use serde_bytes::ByteBuf;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Order {
    id: i64,
    label: String,
}

fn order(id: i64, label: &str) -> Order {
    Order {
        id,
        label: label.to_owned(),
    }
}

/// `rows` as the plugin holds them, each beside its key.
fn keyed(rows: &[Order]) -> Vec<KeyedRow> {
    rows.iter()
        .map(|row| {
            KeyedRow::new(
                encode(&row.id).expect("encode the key"),
                encode(row).expect("encode the row"),
            )
        })
        .collect()
}

fn key(id: i64) -> Vec<u8> {
    encode(&id).expect("encode the key")
}

/// The diff from `base` to `next`, checked to rebuild `next` in order when
/// applied to `base`.
fn round_trip(base: &[Order], next: &[Order]) -> KeyedDiff {
    let mut held = keyed(base);
    let diff = KeyedDiff::between(&held, &keyed(next)).expect("the answers differ");
    diff.clone()
        .apply(&mut held)
        .expect("the diff applies to its base");
    let rebuilt: Vec<Order> = decode(&rows_array(&held)).expect("the rows decode");
    assert_eq!(rebuilt, next, "the page rebuilds the new answer in order");
    diff
}

fn upserted_keys(diff: &KeyedDiff) -> Vec<Vec<u8>> {
    diff.upserted.iter().map(|row| row.key.to_vec()).collect()
}

#[test]
fn a_row_added_at_the_end_is_sent_alone() {
    let diff = round_trip(
        &[order(1, "a"), order(2, "b")],
        &[order(1, "a"), order(2, "b"), order(3, "c")],
    );
    assert_eq!(upserted_keys(&diff), vec![key(3)]);
    assert_eq!(diff.removed, Vec::<ByteBuf>::new());
    assert_eq!(diff.order, None, "an appended row keeps the order");
}

#[test]
fn a_row_added_in_the_middle_carries_the_order() {
    let diff = round_trip(
        &[order(1, "a"), order(2, "b")],
        &[order(1, "a"), order(3, "c"), order(2, "b")],
    );
    assert_eq!(upserted_keys(&diff), vec![key(3)]);
    assert!(diff.order.is_some(), "the new row sits before an old one");
}

#[test]
fn a_changed_row_is_sent_with_its_new_value() {
    let diff = round_trip(
        &[order(1, "a"), order(2, "b")],
        &[order(1, "edited"), order(2, "b")],
    );
    assert_eq!(upserted_keys(&diff), vec![key(1)]);
    assert_eq!(diff.removed, Vec::<ByteBuf>::new());
    assert_eq!(diff.order, None);
}

#[test]
fn a_removed_row_is_named_by_its_key() {
    let diff = round_trip(
        &[order(1, "a"), order(2, "b"), order(3, "c")],
        &[order(1, "a"), order(3, "c")],
    );
    assert_eq!(diff.upserted, Vec::<KeyedRow>::new());
    assert_eq!(
        diff.removed
            .iter()
            .map(|removed| removed.to_vec())
            .collect::<Vec<_>>(),
        vec![key(2)]
    );
    assert_eq!(diff.order, None);
}

#[test]
fn a_moved_row_sends_the_order_alone() {
    let diff = round_trip(
        &[order(1, "a"), order(2, "b"), order(3, "c")],
        &[order(3, "c"), order(1, "a"), order(2, "b")],
    );
    assert_eq!(
        diff.upserted,
        Vec::<KeyedRow>::new(),
        "no row's value changed"
    );
    assert_eq!(diff.removed, Vec::<ByteBuf>::new());
    assert_eq!(
        diff.order,
        Some(
            vec![key(3), key(1), key(2)]
                .into_iter()
                .map(Into::into)
                .collect()
        )
    );
}

#[test]
fn rows_added_changed_removed_and_moved_at_once_rebuild_in_order() {
    round_trip(
        &[order(1, "a"), order(2, "b"), order(3, "c"), order(4, "d")],
        &[
            order(5, "e"),
            order(3, "edited"),
            order(1, "a"),
            order(4, "d"),
        ],
    );
}

#[test]
fn equal_answers_have_no_diff() {
    let rows = keyed(&[order(1, "a"), order(2, "b")]);
    assert!(KeyedDiff::between(&rows, &rows).is_none());
}

#[test]
fn a_diff_removing_a_key_the_base_lacks_is_refused() {
    let mut held = keyed(&[order(1, "a")]);
    let diff = KeyedDiff::between(&keyed(&[order(2, "b")]), &[]).expect("a removal");
    assert!(matches!(
        diff.apply(&mut held),
        Err(ClientError::Protocol(_))
    ));
}

#[test]
fn an_order_naming_other_keys_than_the_base_is_refused() {
    let mut held = keyed(&[order(1, "a"), order(2, "b")]);
    let diff = KeyedDiff::between(
        &keyed(&[order(1, "a"), order(3, "c")]),
        &keyed(&[order(3, "c"), order(1, "a")]),
    )
    .expect("a move");
    assert!(matches!(
        diff.apply(&mut held),
        Err(ClientError::Protocol(_))
    ));
}
