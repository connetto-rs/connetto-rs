//! Placement among linked devices running their media drivers: an author's
//! replicated photo pulled by three devices of three accounts with a device
//! without room left out, a copy above the count released, and a copy
//! marked leaving under pressure handed to another device and deleted (R79
//! decisions 12 to 16).

use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::peer::PeerAuthor;
use connetto_file_client::{ContentEvent, FileId, Resolved};
use tokio::sync::broadcast;

use crate::peer_support::{
    Device, Devices, Relay, apply_entry, beside_all, captured_photo, custody_state,
    declare_replicated, held_chunks, relay, replica, stage,
};
use crate::retention::pseudorandom;
use crate::support::Scripted;

/// How long placement may take to settle, a few of the 10 second room
/// checks a pressed copy waits for.
const BOUND: Duration = Duration::from_secs(60);

/// The room the members with room have for custody.
const ROOMY: u64 = 64 * 1024 * 1024;

/// One device other than the author.
struct Member {
    account: &'static str,
    client: ConnettoClient<Scripted>,
    content: Device,
}

/// An author holding a replicated photo, five devices of three accounts
/// that applied its provisional row, the last without room, every device
/// linked to every other on loopback.
struct Group {
    _dirs: Vec<tempfile::TempDir>,
    author: Device,
    members: Vec<Member>,
    /// The author's relay first, then each member's.
    relays: Vec<Relay>,
    file_id: FileId,
}

impl Group {
    async fn new() -> Self {
        let roster: [(&'static str, u64); 5] = [
            ("anna", ROOMY),
            ("tester", ROOMY),
            ("tester", ROOMY),
            ("curator", ROOMY),
            ("curator", 0),
        ];
        let dirs: Vec<tempfile::TempDir> = (0..roster.len() + 2)
            .map(|_| tempfile::tempdir().expect("temp dir"))
            .collect();
        let (_, author) = declare_replicated(replica(dirs[0].path(), None).await).await;
        let file_id = stage(&author, 1, "anna", &pseudorandom(71, 300_000)).await;
        let devices = Devices::new();
        let author_peer = devices.device("anna", 1);
        let author_key = PeerAuthor {
            key_id: author_peer.identity.key().as_bytes().to_vec(),
            account: "anna".to_owned(),
            bot_list: Some("*".to_owned()),
        };
        let body = captured_photo(dirs[1].path(), 7, file_id.as_bytes().to_vec(), None).await;

        let mut members = Vec::new();
        let mut nodes = vec![devices.node(&author_peer)];
        for (index, (account, budget)) in roster.into_iter().enumerate() {
            let (client, content) = replica(dirs[index + 2].path(), None).await;
            let content = content.with_custody_budget(budget).with_custody_reserve(0);
            let (client, content) = declare_replicated((client, content)).await;
            apply_entry(&client, &author_key, 1, body.clone()).await;
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
        let relays = nodes
            .into_iter()
            .map(|(node, _, events)| relay(node, events))
            .collect();
        Self {
            _dirs: dirs,
            author,
            members,
            relays,
            file_id,
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

    /// Each member's custody state of the photo.
    async fn states(&self) -> Vec<Option<String>> {
        let mut states = Vec::new();
        for member in &self.members {
            states.push(custody_state(&member.client, self.file_id).await);
        }
        states
    }

    /// Waits until the members' custody states satisfy `settled`, read
    /// again whenever the author hears the count of devices keeping its
    /// photo change.
    async fn until(
        &self,
        heard: &mut broadcast::Receiver<ContentEvent>,
        settled: impl Fn(&[Option<String>]) -> bool,
    ) -> Vec<Option<String>> {
        tokio::time::timeout(BOUND, async {
            loop {
                let states = self.states().await;
                if settled(&states) {
                    return states;
                }
                loop {
                    match heard.recv().await {
                        Ok(ContentEvent::Replicated { file_id, .. }) if file_id == self.file_id => {
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
            assert_eq!(
                custody_state(&group.members[extra].client, group.file_id)
                    .await
                    .as_deref(),
                Some("held"),
                "the copy fetched on demand fits, so it enters custody as a fourth"
            );
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
            let states = group.until(&mut heard, |states| kept_by(states, 3)).await;
            assert!(
                states[3].is_some(),
                "the only curator with room holds a copy"
            );
        })
        .await;

    // The curator starts again with a budget below what it holds.
    let pressed = group.members.remove(3);
    group.members.insert(
        3,
        Member {
            content: pressed.content.with_custody_budget(1),
            ..pressed
        },
    );
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
