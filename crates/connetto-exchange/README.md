# connetto-exchange

[![Tests](https://github.com/connetto-rs/connetto-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/connetto-rs/connetto-rs/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/connetto-rs/connetto-rs/blob/main/LICENSE)
[![Coverage](https://codecov.io/gh/connetto-rs/connetto-rs/graph/badge.svg)](https://codecov.io/gh/connetto-rs/connetto-rs)

The device-to-device exchange of signed writer chains, without I/O. Each device offers the head of its own writer's hash chain under its device key's signature. A linked peer that is behind requests the missing range by its first and last sequence, verifies the manifest's chain against the signed head before any body arrives, and applies each body checked against its digest. The crate holds the messages and the state machine of both roles. It reads no database, opens no socket and starts no timer, so the caller feeds it link events, messages and the monotonic clock, and carries out the actions it returns.

A range crosses as manifest messages of at most `MAX_MANIFEST_ROWS` rows, and an entry's body crosses only when the requester's build can apply it.

```rust
use connetto_core::exchange::ManifestRow;
use connetto_core::schema::{Major, SchemaVersion};
use connetto_exchange::{MAX_MANIFEST_ROWS, Message, Transfer, captured_under, manifest};

let major = Major::new(2).expect("a major");
let rows: Vec<ManifestRow> = (1..=MAX_MANIFEST_ROWS as u64 + 1)
    .map(|seq| ManifestRow { seq, major, origin: None, digest: [0; 32] })
    .collect();
let messages: Vec<Message> = manifest(1, [0; 32], rows).collect();
assert_eq!(messages.len(), 2);
assert!(matches!(messages[1], Message::Transfer(Transfer::Manifest { last: true, .. })));

let build = (major, SchemaVersion::from_hash(vec![7; 32]));
assert!(captured_under(&[build.clone()], major, &build));
```

The state machine drives one exchange from both ends. Each side offers its own head when the link comes up, requests the range it is missing under its own build, verifies the manifest's chain against the signed head before any body arrives, and applies each body checked against its digest. The driver feeds the two machines the link's messages in the order the link carries them and carries out the actions they return.

```rust
use std::time::Instant;

use connetto_core::chain::{entry_hash, GENESIS};
use connetto_core::exchange::{EndOutcome, Frontier, ManifestRow, Offer, SignedOffer};
use connetto_core::schema::{Major, SchemaVersion};
use connetto_exchange::{Action, Exchange, Message, Transfer};
use sha2::{Digest, Sha256};

let build = (
    Major::new(2).expect("a major"),
    SchemaVersion::from_hash(vec![7; 32]),
);

let chain = |count: u64| {
    let mut head = GENESIS;
    let mut rows = Vec::new();
    let mut bodies = Vec::new();
    for seq in 1..=count {
        let body = format!("entry {seq}");
        bodies.push(body.clone().into_bytes());
        head = entry_hash(&head, seq, Some(build.0), None, body.as_bytes());
        rows.push(ManifestRow {
            seq,
            major: build.0,
            origin: None,
            digest: Sha256::digest(body.as_bytes()).into(),
        });
    }
    (rows, bodies, head)
};

let offer = |rows: &[ManifestRow], head: [u8; 32], signature: u8| SignedOffer {
    offer: Offer {
        builds: vec![build.clone()],
        settled: None,
        newest: Some(Frontier {
            seq: rows.last().expect("a row").seq,
            head,
        }),
        retractions: Vec::new(),
    },
    signature: vec![signature],
};

let (rows_a, bodies_a, head_a) = chain(2);
let (rows_b, bodies_b, head_b) = chain(1);
let offer_a = offer(&rows_a, head_a, 1);
let offer_b = offer(&rows_b, head_b, 2);

let mut a = Exchange::<&str>::new(Some(build.clone()));
let mut b = Exchange::<&str>::new(Some(build.clone()));

// The link comes up and each side hands over its own offer.
a.linked("b", Frontier { seq: 0, head: GENESIS });
b.linked("a", Frontier { seq: 0, head: GENESIS });
let now = Instant::now();
assert!(matches!(
    a.set_own_offer(offer_a.clone())[0],
    Action::Send { peer: "b", .. }
));
assert!(matches!(
    b.set_own_offer(offer_b.clone())[0],
    Action::Send { peer: "a", .. }
));

// Each side adopts the other's offer and requests the range above its cursor.
let actions = b.offer(&"a", offer_a.clone(), true, now);
assert!(matches!(actions[0], Action::Adopt { .. }));
assert!(matches!(
    actions[1],
    Action::Send {
        message: Message::Transfer(Transfer::Request {
            request_id: 1,
            from: 1,
            to: 2,
            ..
        }),
        ..
    }
));
let actions = a.offer(&"b", offer_b.clone(), true, now);
assert!(matches!(
    actions[1],
    Action::Send {
        message: Message::Transfer(Transfer::Request {
            request_id: 1,
            from: 1,
            to: 1,
            ..
        }),
        ..
    }
));

// Each request turns into a serve.
let actions = a.message(
    &"b",
    Transfer::Request {
        request_id: 1,
        from: 1,
        to: 2,
        build: build.clone(),
    },
    now,
);
assert!(matches!(actions[0], Action::Serve { request_id: 1, .. }));
let actions = b.message(
    &"a",
    Transfer::Request {
        request_id: 1,
        from: 1,
        to: 1,
        build: build.clone(),
    },
    now,
);
assert!(matches!(actions[0], Action::Serve { request_id: 1, .. }));

// A's manifest verifies against its signed head, then its bodies apply.
assert!(
    b.message(
        &"a",
        Transfer::Manifest {
            request_id: 1,
            previous: GENESIS,
            rows: rows_a.clone(),
            last: true,
        },
        now
    )
    .is_empty()
);
for (row, body) in rows_a.iter().zip(&bodies_a) {
    let actions = b.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: row.seq,
            changeset: body.clone(),
        },
        now,
    );
    assert!(matches!(actions[0], Action::Apply { .. }));
}
assert!(
    b.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now
    )
    .is_empty()
);
assert!(a.served(&"b", 1).is_empty());

// B's manifest and body cross the other way.
assert!(
    a.message(
        &"b",
        Transfer::Manifest {
            request_id: 1,
            previous: GENESIS,
            rows: rows_b.clone(),
            last: true,
        },
        now
    )
    .is_empty()
);
let actions = a.message(
    &"b",
    Transfer::Body {
        request_id: 1,
        seq: 1,
        changeset: bodies_b[0].clone(),
    },
    now,
);
assert!(matches!(actions[0], Action::Apply { .. }));
assert!(
    a.message(
        &"b",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now
    )
    .is_empty()
);
assert!(b.served(&"a", 1).is_empty());
```
