//! Placement among linked devices running their media drivers: an author's
//! replicated photo pulled by three devices of three accounts with a device
//! without room left out, a copy above the count released, a copy marked
//! leaving under pressure handed to another device and deleted, only as many
//! copies leaving as the shortfall needs, a pressed copy nobody can take
//! raising `CustodyPressed`, a mark nobody can answer withdrawn, a pull a
//! busy author turned away placed again, and a row applied after the
//! driver's last pass placed (R79 decisions 12 to 16).

use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::peer::PeerAuthor;
use connetto_exchange::media::Media;
use connetto_file_client::{ContentEvent, FileId, Resolved};
use connetto_peer::{CloseReason, Node};
use tokio::sync::{broadcast, mpsc};

use crate::peer_support::{
    Device, Devices, Fault, Relay, apply_entry, bare_holder, bare_holder_of, beside, beside_all,
    captured_photo, custody_state, declare_replicated, held_chunks, listener, port, relay, replica,
    set_custody_state, stage,
};
use crate::retention::pseudorandom;
use crate::support::{Scripted, learn_file_id};

/// How long placement may take to settle, a few of the 10 second room
/// checks a pressed copy waits for.
const BOUND: Duration = Duration::from_secs(60);

/// The room the members with room have for custody.
const ROOMY: u64 = 64 * 1024 * 1024;

/// The size of every photo the author stages.
const PHOTO: usize = 300_000;

/// Five devices of three accounts, the last without room.
const FIVE: [(&str, u64); 5] = [
    ("anna", ROOMY),
    ("tester", ROOMY),
    ("tester", ROOMY),
    ("curator", ROOMY),
    ("curator", 0),
];

/// One device of each account with room and one without, so a copy the
/// three hold has nobody else to go to.
const THREE: [(&str, u64); 4] = [
    ("anna", ROOMY),
    ("tester", ROOMY),
    ("curator", ROOMY),
    ("curator", 0),
];

/// One device other than the author.
struct Member {
    account: &'static str,
    client: ConnettoClient<Scripted>,
    content: Device,
}

/// An author holding replicated photos and devices that applied their
/// provisional rows, every device linked to every other on loopback.
struct Group {
    _dirs: Vec<tempfile::TempDir>,
    author: Device,
    members: Vec<Member>,
    /// The author's relay first, then each member's.
    relays: Vec<Relay>,
    /// The first photo.
    file_id: FileId,
    /// Every photo, the first one first.
    files: Vec<FileId>,
    /// The author's key id, which the members' provisional rows name.
    author_key: Vec<u8>,
    /// The deployment the devices were issued in.
    devices: Devices,
    /// Each member's listening address.
    addrs: Vec<std::net::SocketAddr>,
}

impl Group {
    /// One photo among the [`FIVE`].
    async fn new() -> Self {
        Self::with(&FIVE, 1).await
    }

    /// `photos` photos among the devices of `roster`, each an account and
    /// the custody budget its device has.
    async fn with(roster: &[(&'static str, u64)], photos: usize) -> Self {
        let dirs: Vec<tempfile::TempDir> = (0..roster.len() + 1 + photos)
            .map(|_| tempfile::tempdir().expect("temp dir"))
            .collect();
        let (_, author) = declare_replicated(replica(dirs[0].path(), None).await).await;
        let mut files = Vec::new();
        for photo in 0..photos {
            let id = i64::try_from(photo).expect("few photos");
            let seed = u32::try_from(71 + photo).expect("few photos");
            files.push(stage(&author, id + 1, "anna", &pseudorandom(seed, PHOTO)).await);
        }
        let devices = Devices::new();
        let author_peer = devices.device("anna", 1);
        let author_key = PeerAuthor {
            key_id: author_peer.identity.key().as_bytes().to_vec(),
            account: "anna".to_owned(),
            bot_list: Some("*".to_owned()),
        };
        let mut bodies = Vec::new();
        for (photo, file_id) in files.iter().enumerate() {
            let row = 7 + i64::try_from(photo).expect("few photos");
            let dir = dirs[1 + photo].path();
            bodies.push(captured_photo(dir, row, file_id.as_bytes().to_vec(), None).await);
        }

        let mut members = Vec::new();
        let mut nodes = vec![devices.node(&author_peer)];
        for (index, &(account, budget)) in roster.iter().enumerate() {
            let (client, content) = replica(dirs[index + 1 + photos].path(), None).await;
            let content = content.with_custody_budget(budget).with_custody_reserve(0);
            let (client, content) = declare_replicated((client, content)).await;
            for (seq, body) in (1..).zip(&bodies) {
                apply_entry(&client, &author_key, seq, body.clone()).await;
            }
            members.push(Member {
                account,
                client,
                content,
            });
            let serial = u8::try_from(index + 2).expect("few devices");
            nodes.push(devices.node(&devices.device(account, serial)));
        }
        for (index, (node, _, _)) in nodes.iter().enumerate() {
            for (_, addr, _) in &nodes[index + 1..] {
                node.link(*addr).await.expect("the link completes");
            }
        }
        let addrs = nodes[1..].iter().map(|(_, addr, _)| *addr).collect();
        let relays = nodes
            .into_iter()
            .map(|(node, _, events)| relay(node, events))
            .collect();
        Self {
            _dirs: dirs,
            author,
            members,
            relays,
            file_id: files[0],
            files,
            author_key: author_key.key_id,
            devices,
            addrs,
        }
    }

    /// Runs `proof` beside every device's media driver, each on a fresh
    /// port.
    async fn run(&self, proof: impl Future<Output = ()>) {
        let mut drivers = vec![(&self.author, self.relays[0].port())];
        for (member, relay) in self.members.iter().zip(&self.relays[1..]) {
            drivers.push((&member.content, relay.port()));
        }
        beside_all(drivers, proof).await;
    }

    /// Runs `proof` beside the members' media drivers alone.
    async fn run_members(&self, proof: impl Future<Output = ()>) {
        let drivers = self
            .members
            .iter()
            .zip(&self.relays[1..])
            .map(|(member, relay)| (&member.content, relay.port()))
            .collect();
        beside_all(drivers, proof).await;
    }

    /// A bare device of the `tester` account linked to every member, with
    /// the inbox that receives every report they send it.
    async fn observer(&self) -> (Node, mpsc::UnboundedReceiver<Media>) {
        let (node, _, events) = self.devices.node(&self.devices.device("tester", 9));
        for addr in &self.addrs {
            node.link(*addr).await.expect("the link completes");
        }
        (node, listener(events))
    }

    /// Waits until the members' custody states satisfy `settled`, read
    /// again whenever a member reports to `inbox`.
    async fn until_reported(
        &self,
        inbox: &mut mpsc::UnboundedReceiver<Media>,
        settled: impl Fn(&[Option<String>]) -> bool,
    ) -> Vec<Option<String>> {
        tokio::time::timeout(BOUND, async {
            loop {
                let states = self.states().await;
                if settled(&states) {
                    return states;
                }
                inbox.recv().await.expect("the observer's links stand");
            }
        })
        .await
        .expect("placement settles within the bound")
    }

    /// Settles the author's entry on every member, as a courier's answer
    /// or a verdict would, the server not holding the photo yet.
    async fn settle(&self) {
        let last = u64::try_from(self.files.len()).expect("few photos");
        for member in &self.members {
            let author = self.author_key.clone();
            member
                .client
                .with_conn(move |conn| conn.adjudicate_peer_frontier(&author, last))
                .await
                .expect("the replica is open")
                .expect("adjudicate the author's frontier");
        }
    }

    /// Each member's custody state of the first photo.
    async fn states(&self) -> Vec<Option<String>> {
        self.states_of(self.file_id).await
    }

    /// Each member's custody state of `file_id`.
    async fn states_of(&self, file_id: FileId) -> Vec<Option<String>> {
        let mut states = Vec::new();
        for member in &self.members {
            states.push(custody_state(&member.client, file_id).await);
        }
        states
    }

    /// Waits until the members' custody states of the first photo satisfy
    /// `settled`, as [`Self::until_all`] does.
    async fn until(
        &self,
        heard: &mut broadcast::Receiver<ContentEvent>,
        settled: impl Fn(&[Option<String>]) -> bool,
    ) -> Vec<Option<String>> {
        let mut all = self.until_all(heard, |all| settled(&all[0])).await;
        all.swap_remove(0)
    }

    /// Waits until the members' custody states of every photo, one list per
    /// photo, satisfy `settled`, read again whenever the author hears the
    /// count of devices keeping one of its photos change.
    async fn until_all(
        &self,
        heard: &mut broadcast::Receiver<ContentEvent>,
        settled: impl Fn(&[Vec<Option<String>>]) -> bool,
    ) -> Vec<Vec<Option<String>>> {
        tokio::time::timeout(BOUND, async {
            loop {
                let mut all = Vec::new();
                for file_id in &self.files {
                    all.push(self.states_of(*file_id).await);
                }
                if settled(&all) {
                    return all;
                }
                loop {
                    match heard.recv().await {
                        Ok(ContentEvent::Replicated { file_id, .. })
                            if self.files.contains(&file_id) =>
                        {
                            break;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => break,
                        Err(broadcast::error::RecvError::Closed) => {
                            panic!("the author's events end")
                        }
                        Ok(_) => {}
                    }
                }
            }
        })
        .await
        .expect("placement settles within the bound")
    }
}

/// Whether exactly `copies` members hold the photo and none is leaving.
fn kept_by(states: &[Option<String>], copies: usize) -> bool {
    states.iter().flatten().count() == copies
        && states.iter().flatten().all(|state| state == "held")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_accounts_each_keep_a_copy_and_a_device_without_room_none() {
    let group = Group::new().await;
    let mut heard = group.author.events();
    group
        .run(async {
            let states = group.until(&mut heard, |states| kept_by(states, 3)).await;
            let mut accounts: Vec<&str> = group
                .members
                .iter()
                .zip(&states)
                .filter(|(_, state)| state.is_some())
                .map(|(member, _)| member.account)
                .collect();
            accounts.sort_unstable();
            assert_eq!(
                accounts,
                ["anna", "curator", "tester"],
                "each account's best device first"
            );
            assert_eq!(states[4], None, "the device without room takes nothing");
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_above_the_count_is_released() {
    let group = Group::new().await;
    let mut heard = group.author.events();
    group
        .run(async {
            let states = group.until(&mut heard, |states| kept_by(states, 3)).await;
            let extra = (0..4)
                .find(|index| states[*index].is_none())
                .expect("a member with room holds nothing");
            assert!(matches!(
                group.members[extra]
                    .content
                    .resolve(group.file_id)
                    .await
                    .expect("resolve"),
                Resolved::Local { .. }
            ));
            // The author hears four holders, placement marking the fourth
            // leaving as surplus at any moment after it entered custody.
            tokio::time::timeout(BOUND, async {
                loop {
                    if let Ok(ContentEvent::Replicated {
                        file_id,
                        holders,
                        leaving,
                    }) = heard.recv().await
                        && file_id == group.file_id
                        && holders + leaving == 4
                    {
                        return;
                    }
                }
            })
            .await
            .expect("the copy fetched on demand fits, so it enters custody as a fourth");
            group.until(&mut heard, |states| kept_by(states, 3)).await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_pressed_out_of_its_budget_is_handed_on_and_deleted() {
    let mut group = Group::new().await;
    let mut heard = group.author.events();
    group
        .run(async {
            // Each account's best device settles in, the only curator with
            // room among them.
            group
                .until(&mut heard, |states| {
                    kept_by(states, 3) && states[3].is_some()
                })
                .await;
        })
        .await;

    // The curator starts again with a budget below what it holds.
    restart(&mut group, 3, 1);
    group
        .run(async {
            let states = group
                .until(&mut heard, |states| {
                    states[3].is_none() && kept_by(states, 3)
                })
                .await;
            assert_eq!(states[4], None, "the device without room takes nothing");
            assert_eq!(
                held_chunks(&group.members[3].client, group.file_id).await,
                0,
                "the pressed copy's bytes are gone once another device holds it"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settled_copy_pressed_out_of_its_budget_is_handed_on_before_the_server_holds_it() {
    let mut group = Group::new().await;
    let mut heard = group.author.events();
    group
        .run(async {
            group
                .until(&mut heard, |states| {
                    kept_by(states, 3) && states[3].is_some()
                })
                .await;
        })
        .await;
    group.settle().await;
    // The author leaves with its row settled and its file not yet on the
    // server, so only the members hold it.
    group.relays[0].node().stop(CloseReason::Closed);

    // The curator starts again with a budget below what it holds.
    restart(&mut group, 3, 1);
    let (_observer, mut inbox) = group.observer().await;
    group
        .run_members(async {
            let states = group
                .until_reported(&mut inbox, |states| {
                    states[3].is_none() && kept_by(states, 3)
                })
                .await;
            assert_eq!(states[4], None, "the device without room takes nothing");
            assert_eq!(
                held_chunks(&group.members[3].client, group.file_id).await,
                0,
                "the pressed copy's bytes are gone once another device holds it"
            );
        })
        .await;
}

/// A member started again with `budget` bytes for custody, keeping its
/// replica and its links.
fn restart(group: &mut Group, member: usize, budget: u64) {
    let started = group.members.remove(member);
    group.members.insert(
        member,
        Member {
            content: started.content.with_custody_budget(budget),
            ..started
        },
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mark_nobody_can_answer_is_withdrawn() {
    let group = Group::with(&THREE, 1).await;
    let mut heard = group.author.events();
    group
        .run(async {
            group.until(&mut heard, |states| kept_by(states, 3)).await;
        })
        .await;
    // The tester's copy is marked leaving as surplus, yet nobody else has
    // room to take it and the count needs all three.
    set_custody_state(&group.members[1].client, group.file_id, "leaving").await;
    group
        .run(async {
            group.until(&mut heard, |states| kept_by(states, 3)).await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pressed_copy_nobody_can_take_stays_and_raises_custody_pressed() {
    let mut group = Group::with(&THREE, 1).await;
    let mut heard = group.author.events();
    group
        .run(async {
            group.until(&mut heard, |states| kept_by(states, 3)).await;
        })
        .await;

    restart(&mut group, 0, 1);
    let mut pressed = group.members[0].content.events();
    group
        .run(async {
            let held_bytes = tokio::time::timeout(BOUND, async {
                loop {
                    if let Ok(ContentEvent::CustodyPressed { held_bytes }) = pressed.recv().await {
                        return held_bytes;
                    }
                }
            })
            .await
            .expect("the stranded copy raises CustodyPressed");
            assert_eq!(held_bytes, u64::try_from(PHOTO).expect("fits"));
            assert_eq!(
                custody_state(&group.members[0].client, group.file_id)
                    .await
                    .as_deref(),
                Some("pressed"),
                "the copy stays, marked leaving"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pressure_hands_on_only_as_many_copies_as_the_shortfall_needs() {
    let mut group = Group::with(&FIVE, 2).await;
    let mut heard = group.author.events();
    let mut both = None;
    group
        .run(async {
            let all = group
                .until_all(&mut heard, |all| {
                    all.iter().all(|states| kept_by(states, 3))
                })
                .await;
            both = (0..4).find(|member| all.iter().all(|states| states[*member].is_some()));
        })
        .await;
    let member = both.expect("with three copies each among four, a member holds both");

    // A budget of one photo leaves the member one photo over.
    restart(&mut group, member, u64::try_from(PHOTO).expect("fits"));
    group
        .run(async {
            let all = group
                .until_all(&mut heard, |all| {
                    all.iter().all(|states| kept_by(states, 3))
                        && all.iter().filter(|states| states[member].is_some()).count() == 1
                })
                .await;
            assert_eq!(all.len(), 2);
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_a_busy_author_turned_away_is_placed_again() {
    let dirs = [
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    ];
    let bytes = pseudorandom(77, PHOTO);
    let file_id = learn_file_id(&bytes).await;
    let devices = Devices::new();
    let author_device = devices.device("anna", 1);
    let (author_node, author_addr, author_events) = devices.node(&author_device);
    let _author = bare_holder(author_node, author_events, bytes, Fault::BusyFirst);
    let author = PeerAuthor {
        key_id: author_device.identity.key().as_bytes().to_vec(),
        account: "anna".to_owned(),
        bot_list: Some("*".to_owned()),
    };
    let (client, content) = replica(dirs[0].path(), None).await;
    let content = content.with_custody_budget(ROOMY).with_custody_reserve(0);
    let (client, content) = declare_replicated((client, content)).await;
    let body = captured_photo(dirs[1].path(), 7, file_id.as_bytes().to_vec(), None).await;
    apply_entry(&client, &author, 1, body).await;
    let (node, _, events) = devices.node(&devices.device("tester", 2));
    node.link(author_addr).await.expect("the link completes");

    let mut fetched = content.events();
    beside(&content, port(node, events), async {
        tokio::time::timeout(BOUND, async {
            loop {
                if let Ok(ContentEvent::Fetched { file_id: got }) = fetched.recv().await
                    && got == file_id
                {
                    return;
                }
            }
        })
        .await
        .expect("the pull is placed again once the author is free");
        assert_eq!(
            custody_state(&client, file_id).await.as_deref(),
            Some("held")
        );
    })
    .await;
}

/// A device whose driver already placed with nothing to rank pulls the
/// photo of a peer entry applied afterwards, with no other event between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_applied_after_the_last_pass_is_placed() {
    let dirs = [
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    ];
    let bytes = pseudorandom(78, PHOTO);
    let file_id = learn_file_id(&bytes).await;
    let devices = Devices::new();
    let author_device = devices.device("anna", 1);
    let (author_node, author_addr, author_events) = devices.node(&author_device);
    let (_asked, mut inbox) = bare_holder_of(author_node, author_events, vec![bytes], Fault::None);
    let author = PeerAuthor {
        key_id: author_device.identity.key().as_bytes().to_vec(),
        account: "anna".to_owned(),
        bot_list: Some("*".to_owned()),
    };
    let (client, content) = replica(dirs[0].path(), None).await;
    let content = content.with_custody_budget(ROOMY).with_custody_reserve(0);
    let (client, content) = declare_replicated((client, content)).await;
    let (node, _, events) = devices.node(&devices.device("tester", 2));
    node.link(author_addr).await.expect("the link completes");

    let mut fetched = content.events();
    Box::pin(beside(&content, port(node, events), async {
        // The opening report leaves once the driver queued its first pass,
        // whose read of the replica holds the connection ahead of the apply.
        tokio::time::timeout(BOUND, async {
            while !matches!(inbox.recv().await, Some(Media::Holdings { .. }) | None) {}
        })
        .await
        .expect("the driver opens the link");
        let body = captured_photo(dirs[1].path(), 7, file_id.as_bytes().to_vec(), None).await;
        apply_entry(&client, &author, 1, body).await;
        tokio::time::timeout(BOUND, async {
            loop {
                if let Ok(ContentEvent::Fetched { file_id: got }) = fetched.recv().await
                    && got == file_id
                {
                    return;
                }
            }
        })
        .await
        .expect("the applied row is placed");
        assert_eq!(
            custody_state(&client, file_id).await.as_deref(),
            Some("held")
        );
    }))
    .await;
}
