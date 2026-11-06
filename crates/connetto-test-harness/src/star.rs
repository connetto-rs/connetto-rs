//! The parts of the handheld star run (R76 slice 9f) that stand without a
//! device: the round's plan, the three-way link's matrix off the pages'
//! linked lines, and the run's summary. The walk that drives the iPhone, the
//! iPad and the Android phone lives in the `connetto-ios-proof` runner.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// The three handhelds of the star.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Device {
    /// The phone the run proves first, which joins the round.
    IPhone,
    /// The tablet, which joins the round.
    IPad,
    /// The phone the runner drives over `adb`, which hosts the round.
    Android,
}

impl Device {
    /// The star's devices, in the matrix's order.
    pub const ALL: [Device; 3] = [Self::IPhone, Self::IPad, Self::Android];

    /// The device's index in [`Device::ALL`].
    pub const fn index(self) -> usize {
        match self {
            Self::IPhone => 0,
            Self::IPad => 1,
            Self::Android => 2,
        }
    }

    /// The device's name in the summary and the evidence.
    pub const fn name(self) -> &'static str {
        match self {
            Self::IPhone => "iphone",
            Self::IPad => "ipad",
            Self::Android => "android",
        }
    }

    /// The account the device signs in as, which its linked lines name it by.
    pub const fn account(self) -> &'static str {
        match self {
            Self::IPhone => "alice",
            Self::IPad => "bob",
            Self::Android => "carol",
        }
    }
}

/// The star's one round (R76 slice 9f): the Android phone hosts its
/// local-only hotspot while the iPhone and the iPad join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Round {
    /// The Android phone hosts the local-only hotspot.
    AndroidHosts,
}

impl Round {
    /// The star's rounds, in the order the run walks them.
    pub const ALL: [Round; 1] = [Self::AndroidHosts];

    /// The round's host.
    pub const fn host(self) -> Device {
        Device::Android
    }

    /// The round's joiners, in the order they join, the first linked before
    /// the second, the tablet last so the phone's page stands ready when
    /// the tablet's link to it is the one still to wait on.
    pub const fn joiners(self) -> [Device; 2] {
        [Device::IPhone, Device::IPad]
    }

    /// The round's name in the summary and the evidence.
    pub const fn name(self) -> &'static str {
        "android-hosts"
    }
}

/// How a joiner joins the host's hotspot: the Bluetooth beacon by default,
/// the pasted `WIFI:` payload or the typed form instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Join {
    /// The host's beacon, read over Bluetooth.
    #[default]
    Beacon,
    /// The host's `WIFI:` payload, pasted.
    Payload,
    /// The host's offer's lines, typed.
    Form,
}
impl Join {
    /// The join's name in the summary and the evidence.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Beacon => "beacon",
            Self::Payload => "payload",
            Self::Form => "form",
        }
    }

    /// The command line's spelling of the join.
    ///
    /// # Errors
    ///
    /// When the name is neither join.
    pub fn parse(name: &str) -> Result<Self, StarError> {
        match name {
            "beacon" => Ok(Self::Beacon),
            "payload" => Ok(Self::Payload),
            "form" => Ok(Self::Form),
            other => Err(StarError::UnknownJoin(other.to_owned())),
        }
    }
}

/// Why the star could not read a join or the Android phone's VPN.
#[derive(Debug, thiserror::Error)]
pub enum StarError {
    /// The command line named no join.
    #[error("unknown join {0}, expected beacon, payload or form")]
    UnknownJoin(String),
    /// `dumpsys connectivity` shows a connected VPN without its owner's uid.
    #[error("the connected VPN names no owner uid")]
    VpnWithoutOwner,
}

/// Whether the connected VPN in `connectivity`, the Android phone's
/// `dumpsys connectivity`, belongs to the app `package` describes, the
/// phone's `dumpsys package com.tailscale.ipn`. No connected VPN, another
/// app's VPN, or no Tailscale installed all answer `false`.
///
/// # Errors
///
/// When a connected VPN names no `OwnerUid`.
pub fn tailscale_holds_vpn(connectivity: &str, package: &str) -> Result<bool, StarError> {
    let Some(vpn) = connectivity
        .lines()
        .find(|line| line.contains("ni{VPN CONNECTED"))
    else {
        return Ok(false);
    };
    let owner = vpn
        .split_once("OwnerUid: ")
        .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next())
        .filter(|uid| !uid.is_empty())
        .ok_or(StarError::VpnWithoutOwner)?;
    Ok(package
        .lines()
        .any(|line| line.trim().strip_prefix("userId=") == Some(owner)))
}

/// One round's plan: who hosts, who joins in which order, and how.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoundPlan {
    /// The round, whose host and joiners the plan restates.
    pub round: Round,
    /// The round's host.
    pub host: Device,
    /// The round's joiners, in join order.
    pub joiners: [Device; 2],
    /// How the joiners join.
    pub join: Join,
}

/// The one round, with the join the joiners take.
pub fn plan(join: Join) -> [RoundPlan; 1] {
    Round::ALL.map(|round| RoundPlan {
        round,
        host: round.host(),
        joiners: round.joiners(),
        join,
    })
}

/// One step's result, for the run's summary.
pub struct StepReport {
    /// The step's name, as the summary shows it.
    pub name: String,
    /// How long the step ran.
    pub took: Duration,
    /// How it ended, its reason where it failed.
    pub outcome: Result<(), String>,
}

/// One round's steps: its two joins in join order, its three-way link, the
/// R77 phase where the round walks it, and its restore.
pub struct RoundReport {
    /// The round the steps walked.
    pub round: Round,
    /// The joiners' joins, in join order.
    pub joins: [StepReport; 2],
    /// The three-way link's check, every device naming the other two.
    pub link: StepReport,
    /// The R77 phase, which walks over the round's standing link.
    pub r77: Option<StepReport>,
    /// The round's restore: the hotspot down and the devices back at rest.
    pub restore: StepReport,
}

impl RoundReport {
    /// The round's steps in the order they ran.
    pub fn steps(&self) -> impl Iterator<Item = &StepReport> {
        self.joins
            .iter()
            .chain([&self.link])
            .chain(&self.r77)
            .chain([&self.restore])
    }
}

/// The three-way link's matrix, which device's page named which other device
/// as linked through the round's hotspot. The round holds once every
/// off-diagonal cell stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkMatrix {
    /// `seen[i][j]`: device `i`'s page named device `j` as linked through the hotspot.
    seen: [[bool; 3]; 3],
}

impl LinkMatrix {
    /// The matrix off the three pages, `identities` naming each device as its
    /// own page's listening line and `pages` holding each device's page text
    /// in [`Device::ALL`]'s order, an edge counting only through `gateway`'s
    /// subnet. A device's own line names itself, which no cell counts.
    pub fn from_pages(identities: [&str; 3], pages: [&str; 3], gateway: Option<Ipv4Addr>) -> Self {
        let mut seen = [[false; 3]; 3];
        for i in 0..3 {
            for j in 0..3 {
                if i == j {
                    continue;
                }
                seen[i][j] = hotspot_links(pages[i], gateway).any(|peer| peer == identities[j]);
            }
        }
        Self { seen }
    }

    /// Whether every device named the other two as linked.
    pub fn holds(&self) -> bool {
        self.seen.iter().enumerate().all(|(i, peers)| {
            peers
                .iter()
                .enumerate()
                .all(|(j, linked)| i == j || *linked)
        })
    }

    /// The missing pairs, `device named no peer`, for the failure's line.
    pub fn missing(&self, names: [&str; 3]) -> Vec<String> {
        let mut missing = Vec::new();
        for i in 0..3 {
            for j in 0..3 {
                if i != j && !self.seen[i][j] {
                    missing.push(format!("{} never named {} as linked", names[i], names[j]));
                }
            }
        }
        missing
    }

    /// The matrix's text, one line per device, for the evidence.
    pub fn text(&self, names: [&str; 3]) -> String {
        (0..3)
            .map(|i| {
                let linked: Vec<&str> = (0..3)
                    .into_iter()
                    .filter(|&j| i != j && self.seen[i][j])
                    .map(|j| names[j])
                    .collect();
                format!("{}: linked {}", names[i], linked.join(", "))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// The gateway a joiner's page names in its `joined through the gateway` line.
pub fn hotspot_gateway(page: &str) -> Option<Ipv4Addr> {
    page.lines().find_map(|line| {
        let rest = line.split_once("joined through the gateway ")?.1;
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// Whether the page names any peer as linked through `gateway`'s subnet.
pub fn names_hotspot_link(page: &str, gateway: Option<Ipv4Addr>) -> bool {
    hotspot_links(page, gateway).next().is_some()
}

/// The peers a page names as linked through an `IPv4` address sharing `gateway`'s first three octets.
fn hotspot_links(page: &str, gateway: Option<Ipv4Addr>) -> impl Iterator<Item = &str> {
    page.lines().filter_map(move |line| {
        let gateway = gateway?;
        let (peer, address) = line.trim().strip_prefix("linked: ")?.split_once(" via ")?;
        let IpAddr::V4(remote) = address.parse::<SocketAddr>().ok()?.ip() else {
            return None;
        };
        (remote.octets()[..3] == gateway.octets()[..3]).then_some(peer)
    })
}

/// One R77 provisional line the demo's team notes panel stands, read off a
/// page dump the way the `linked:` lines are read (R77 slice 8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProvisionalLine {
    /// A peer entry's row applied, the author's account as the provenance names it.
    Applied {
        /// The table the row sits in, as the application names it.
        table: String,
        /// The row's key, the panel's spelling.
        key: String,
        /// The author's account.
        account: String,
    },
    /// A server write confirmed the row, its provenance cleared.
    Confirmed {
        /// The table the row sits in.
        table: String,
        /// The row's key.
        key: String,
    },
    /// A retraction rolled the row back, with the cause the retraction named.
    RolledBack {
        /// The table the row sits in.
        table: String,
        /// The row's key.
        key: String,
        /// The cause the panel says, the refused entry by the server's reason.
        cause: String,
    },
    /// The receiver's policies refused the entry as the author.
    Refused {
        /// The table the entry named.
        table: String,
        /// The entry's key.
        key: String,
    },
}

/// One `provisional:` line as the panel stands it, `None` where the line is not
/// one. The table and the key are single tokens, the applied line's account the
/// field's last token, and the rolled-back line's cause everything after `because`.
fn parse_provisional_line(line: &str) -> Option<ProvisionalLine> {
    let table_key = |fields: &str| -> Option<(String, String)> {
        let mut parts = fields.split(' ');
        let table = parts.next()?;
        let key = parts.next()?;
        (parts.next().is_none() && !table.is_empty() && !key.is_empty())
            .then(|| (table.to_string(), key.to_string()))
    };
    let rest = line.trim().strip_prefix("provisional: ")?;
    if let Some(rest) = rest.strip_prefix("applied ") {
        let (fields, account) = rest.rsplit_once(" by ")?;
        let (table, key) = table_key(fields)?;
        return Some(ProvisionalLine::Applied {
            table,
            key,
            account: account.to_owned(),
        });
    }
    if let Some(rest) = rest.strip_prefix("confirmed ") {
        return table_key(rest).map(|(table, key)| ProvisionalLine::Confirmed { table, key });
    }
    if let Some(rest) = rest.strip_prefix("rolled back ") {
        let (fields, cause) = rest.rsplit_once(" because ")?;
        let (table, key) = table_key(fields)?;
        return Some(ProvisionalLine::RolledBack {
            table,
            key,
            cause: cause.to_owned(),
        });
    }
    rest.strip_prefix("refused ")
        .and_then(table_key)
        .map(|(table, key)| ProvisionalLine::Refused { table, key })
}

/// The provisional lines a page dump carries, in page order, beside the
/// `linked:` lines the matrix reads.
#[must_use]
pub fn provisional_lines(page: &str) -> Vec<ProvisionalLine> {
    page.lines().filter_map(parse_provisional_line).collect()
}

/// One team-note line the panel stands, the note's body and author and, where
/// the row is a peer-applied entry awaiting a verdict, the account the
/// provenance names it from (R77 slice 8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoteLine {
    /// The note's body, as written.
    pub body: String,
    /// The author's id, as the note's row names it.
    pub author: String,
    /// The account a provisional peer write stands from, absent for the
    /// phone's own note and for settled rows.
    pub provisional_from: Option<String>,
}

/// Parse one `note:` line the panel stands, `None` where the line is not one.
/// The author is the line's last field, the body everything before it, and the
/// provisional account the last field inside the trailing mark.
fn parse_note_line(line: &str) -> Option<NoteLine> {
    let rest = line.trim().strip_prefix("note: ")?;
    let (rest, provisional_from) = match rest.strip_suffix(')') {
        Some(inner) => match inner.rsplit_once(" (provisional from ") {
            Some((body_author, account)) => (body_author.to_owned(), Some(account.to_owned())),
            None => (rest.to_owned(), None),
        },
        None => (rest.to_owned(), None),
    };
    let (body, author) = rest.rsplit_once(" by ")?;
    (!body.is_empty() && !author.is_empty()).then(|| NoteLine {
        body: body.to_owned(),
        author: author.to_owned(),
        provisional_from,
    })
}

/// The team-note lines a page dump carries, in page order.
#[must_use]
pub fn note_lines(page: &str) -> Vec<NoteLine> {
    page.lines().filter_map(parse_note_line).collect()
}

/// The count the page's last `tidy:` line reports, the provisional rows the
/// eviction pass kept, `None` where the page stands no tidy line. The content
/// pane's `content tidy:` lines are no match, since they do not start with
/// `tidy:`.
#[must_use]
pub fn tidy_remaining(page: &str) -> Option<usize> {
    page.lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix("tidy: ")?.trim().split(' ').next())
        .and_then(|count| count.parse().ok())
}

/// The run's one-line step results, excluding private page dumps from errors.
/// A run that skipped the single-phone proof names it as not run.
pub fn summary(
    commit: &str,
    proof: Option<&(Duration, Result<(), String>)>,
    rounds: &[RoundReport],
) -> String {
    let line = |name: &str, took: Duration, outcome: &Result<(), String>| match outcome {
        Ok(()) => format!("{name}: passed in {}s\n", took.as_secs()),
        Err(err) => format!(
            "{name}: failed in {}s: {}\n",
            took.as_secs(),
            err.lines().next().unwrap_or_default()
        ),
    };
    let mut text = format!("commit: {commit}\n");
    text += &match proof {
        Some((took, outcome)) => line("single-phone proof", *took, outcome),
        None => "single-phone proof: not run\n".to_owned(),
    };
    for step in rounds.iter().flat_map(RoundReport::steps) {
        text += &line(&step.name, step.took, &step.outcome);
    }
    text
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use super::{
        Device, Join, LinkMatrix, NoteLine, ProvisionalLine, Round, RoundReport, StarError,
        StepReport, hotspot_gateway, names_hotspot_link, note_lines, plan, provisional_lines,
        summary, tailscale_holds_vpn, tidy_remaining,
    };

    /// The M52's VPN agent line, trimmed to the fields the read uses.
    const TAILSCALE_VPN: &str = "  NetworkAgentInfo{network{145}  handle{626175954957}  ni{VPN CONNECTED extra: } lp{{InterfaceName: tun1 Domains: tail0b5119.ts.net}} nc{[ Transports: VPN Capabilities: NOT_METERED&INTERNET OwnerUid: 10522 AdminUids: [10522] TransportInfo: <VpnTransportInfo{type=1, sessionId=null, bypassable=false}>]}";
    const TAILSCALE_PACKAGE: &str =
        "Packages:\n  Package [com.tailscale.ipn] (8c6c1e5):\n    userId=10522\n";

    fn step(name: &str, outcome: Result<(), String>) -> StepReport {
        StepReport {
            name: name.to_owned(),
            took: Duration::from_secs(7),
            outcome,
        }
    }

    #[test]
    fn the_summary_excludes_private_page_contents_from_errors() {
        let page_error = "the peer never linked\nhotspot passphrase: private-network-key\nhotspot payload: WIFI:T:WPA;P:private-network-key;;";
        let text = summary(
            "source",
            Some(&(Duration::ZERO, Err(page_error.to_owned()))),
            &[],
        );
        assert!(text.contains("the peer never linked"));
        assert!(!text.contains("private-network-key"));
        assert!(!text.contains("WIFI:"));
    }

    /// The plan hosts the round by the Android phone, joins with the other
    /// two and carries the join through.
    #[test]
    fn the_plan_covers_the_round_with_all_three_devices() {
        let plans = plan(Join::Payload);
        assert_eq!(plans.len(), 1);
        for plan in &plans {
            let devices = [plan.host, plan.joiners[0], plan.joiners[1]];
            for device in Device::ALL {
                assert!(devices.contains(&device));
            }
            assert!(devices[0] != devices[1] && devices[1] != devices[2]);
            assert_eq!(plan.round.host(), plan.host);
            assert_eq!(plan.round.joiners(), plan.joiners);
            assert_eq!(plan.join, Join::Payload);
        }
        assert_eq!(plans[0].round, Round::AndroidHosts);
        assert_eq!(plans[0].host, Device::Android);
        assert_eq!(plans[0].joiners, [Device::IPhone, Device::IPad]);
    }

    const GATEWAY: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 1);

    fn pages() -> [&'static str; 3] {
        [
            "peer: listening on 4431 as alice/01234567\nlinked: bob/89abcdef via 10.42.0.7:4432\nlinked: carol/12345678 via 10.42.0.1:4433\n",
            "peer: listening on 4432 as bob/89abcdef\nlinked: alice/01234567 via 10.42.0.9:4431\nlinked: carol/12345678 via 10.42.0.1:4433\n",
            "peer: listening on 4433 as carol/12345678\nlinked: alice/01234567 via 10.42.0.9:51000\nlinked: bob/89abcdef via 10.42.0.7:51001\n",
        ]
    }

    /// The matrix holds once every device named the other two through the hotspot.
    #[test]
    fn the_matrix_holds_when_every_device_names_the_other_two() {
        let identities = ["alice/01234567", "bob/89abcdef", "carol/12345678"];
        let pages = pages();
        let matrix = LinkMatrix::from_pages(identities, pages, Some(GATEWAY));
        assert!(matrix.holds());
        assert_eq!(
            matrix.missing(["iphone", "ipad", "android"]),
            Vec::<String>::new()
        );
    }

    /// A missing link fails the matrix and names the pair that is missing.
    #[test]
    fn a_missing_link_fails_the_matrix_and_names_the_pair() {
        let identities = ["alice/01234567", "bob/89abcdef", "carol/12345678"];
        let mut pages = pages();
        pages[0] =
            "peer: listening on 4431 as alice/01234567\nlinked: bob/89abcdef via 10.42.0.7:4432\n";
        let matrix = LinkMatrix::from_pages(identities, pages, Some(GATEWAY));
        assert!(!matrix.holds());
        assert_eq!(
            matrix.missing(["iphone", "ipad", "android"]),
            ["iphone never named android as linked".to_owned()]
        );
    }

    /// A link over any network but the round's hotspot counts for no edge.
    #[test]
    fn a_link_outside_the_hotspot_subnet_counts_for_nothing() {
        let identities = ["alice/01234567", "bob/89abcdef", "carol/12345678"];
        let mut pages = pages();
        pages[0] = "linked: bob/89abcdef via 192.168.1.7:4432\nlinked: carol/12345678 via [fe80::1%7]:4433\n";
        let matrix = LinkMatrix::from_pages(identities, pages, Some(GATEWAY));
        assert_eq!(
            matrix.missing(["iphone", "ipad", "android"]),
            [
                "iphone never named ipad as linked".to_owned(),
                "iphone never named android as linked".to_owned(),
            ]
        );
        assert!(!names_hotspot_link(pages[0], Some(GATEWAY)));
        assert!(names_hotspot_link(pages[1], Some(GATEWAY)));
    }

    /// Without a known gateway no edge counts, and no hotspot link stands.
    #[test]
    fn without_a_gateway_no_edge_counts() {
        let identities = ["alice/01234567", "bob/89abcdef", "carol/12345678"];
        let matrix = LinkMatrix::from_pages(identities, pages(), None);
        assert_eq!(matrix.missing(["iphone", "ipad", "android"]).len(), 6);
        assert!(!names_hotspot_link(pages()[0], None));
    }

    /// A joiner's page names the hotspot's gateway, and a host's page names none.
    #[test]
    fn the_gateway_is_read_off_a_joined_page() {
        assert_eq!(
            hotspot_gateway("hotspot: joined through the gateway 172.20.10.1 as alice/01234567\n"),
            Some(Ipv4Addr::new(172, 20, 10, 1))
        );
        assert_eq!(
            hotspot_gateway("hotspot: joined through the gateway 10.42.0.1\n"),
            Some(GATEWAY)
        );
        assert_eq!(hotspot_gateway("hotspot: hosting\n"), None);
    }

    /// A device's own line names itself, which no cell counts.
    #[test]
    fn a_page_naming_its_own_identity_names_no_peer() {
        let identities = ["alice/01234567", "bob/89abcdef", "carol/12345678"];
        let pages: [&str; 3] = [
            "linked: alice/01234567 via 10.42.0.9:1\n",
            "linked: bob/89abcdef via 10.42.0.7:1\n",
            "linked: carol/12345678 via 10.42.0.1:1\n",
        ];
        let matrix = LinkMatrix::from_pages(identities, pages, Some(GATEWAY));
        assert!(!matrix.holds());
        assert_eq!(matrix.missing(["iphone", "ipad", "android"]).len(), 6);
    }

    /// The matrix's text names, per device, the peers its page named.
    #[test]
    fn the_matrix_text_names_each_devices_linked_peers() {
        let identities = ["alice/01234567", "bob/89abcdef", "carol/12345678"];
        let matrix = LinkMatrix::from_pages(identities, pages(), Some(GATEWAY));
        assert_eq!(
            matrix.text(["iphone", "ipad", "android"]),
            "iphone: linked ipad, android\nipad: linked iphone, android\nandroid: linked iphone, ipad"
        );
    }

    fn round(round: Round) -> RoundReport {
        let (first, second) = (
            "the iphone joins by the beacon",
            "the ipad joins by the beacon",
        );
        let fail = Err("the ipad never named the android as linked".to_owned());
        RoundReport {
            round,
            joins: [
                step(&format!("{} round, {first}", round.name()), Ok(())),
                step(&format!("{} round, {second}", round.name()), fail.clone()),
            ],
            link: step(&format!("{} round, the three-way link", round.name()), fail),
            r77: Some(step(
                &format!("{} round, the R77 phase", round.name()),
                Err("the R77 step never held".to_owned()),
            )),
            restore: step(&format!("{} round, the restore", round.name()), Ok(())),
        }
    }

    /// The summary names the commit, the proof, and each round's steps with
    /// their time and, where one failed, why.
    #[test]
    fn the_summary_names_every_step_and_why_a_step_failed() {
        let rounds = [round(Round::AndroidHosts)];
        let text = summary("abc123", Some(&(Duration::from_secs(60), Ok(()))), &rounds);
        for line in [
            "commit: abc123",
            "single-phone proof: passed in 60s",
            "android-hosts round, the iphone joins by the beacon: passed in 7s",
            "android-hosts round, the ipad joins by the beacon: failed in 7s: the ipad never named the android as linked",
            "android-hosts round, the three-way link: failed in 7s: the ipad never named the android as linked",
            "android-hosts round, the restore: passed in 7s",
            "android-hosts round, the R77 phase: failed in 7s: the R77 step never held",
        ] {
            assert!(text.contains(line), "the summary lacks {line}:\n{text}");
        }
    }

    #[test]
    fn tailscale_holds_the_vpn_only_when_it_owns_the_connected_one() {
        let wifi = "  NetworkAgentInfo{network{151}  ni{WIFI CONNECTED extra: } nc{[ Transports: WIFI OwnerUid: 1000]}";
        let with_vpn = format!("{wifi}\n{TAILSCALE_VPN}\n");
        assert!(tailscale_holds_vpn(&with_vpn, TAILSCALE_PACKAGE).unwrap());
        assert!(!tailscale_holds_vpn(wifi, TAILSCALE_PACKAGE).unwrap());
        let other_owner = with_vpn.replace("OwnerUid: 10522", "OwnerUid: 10377");
        assert!(!tailscale_holds_vpn(&other_owner, TAILSCALE_PACKAGE).unwrap());
        assert!(!tailscale_holds_vpn(&with_vpn, "Packages:\n").unwrap());
        let prefix_uid = TAILSCALE_PACKAGE.replace("userId=10522", "userId=105221");
        assert!(!tailscale_holds_vpn(&with_vpn, &prefix_uid).unwrap());
    }

    #[test]
    fn a_connected_vpn_without_an_owner_is_an_error() {
        let ownerless = TAILSCALE_VPN.replace("OwnerUid: 10522 ", "");
        assert!(matches!(
            tailscale_holds_vpn(&ownerless, TAILSCALE_PACKAGE),
            Err(StarError::VpnWithoutOwner)
        ));
    }

    const NOTE_KEY: &str = "9f1c8001-855d-4b1e-9c2f-73a16b0d4e5a";

    /// The page dump the star walk reads, the peer lines and the panel's lines
    /// beside the rest of the page.
    fn team_page() -> String {
        format!(
            "peer: listening on 4431 as alice/01234567\n\
             linked: bob/89abcdef via 10.42.0.7:4432\n\
             team: camp\n\
             teams: camp\n\
             note: head to the ridge by bob (provisional from bob@demo.dev)\n\
             note: pack the rope by alice\n\
             provisional: applied team_notes {NOTE_KEY} by bob@demo.dev\n\
             provisional: confirmed team_notes {NOTE_KEY}\n\
             provisional: rolled back team_notes {NOTE_KEY} because unauthorized\n\
             provisional: refused team_notes {NOTE_KEY}\n\
             tidy: 2 provisional row(s) remain\n"
        )
    }

    /// Each R77 line stands its table, key and, where the line carries one,
    /// its account or cause.
    #[test]
    fn each_provisional_line_stands_its_fields() {
        let lines = provisional_lines(&team_page());
        assert_eq!(lines.len(), 4);
        assert_eq!(
            lines,
            vec![
                ProvisionalLine::Applied {
                    table: "team_notes".to_owned(),
                    key: NOTE_KEY.to_owned(),
                    account: "bob@demo.dev".to_owned(),
                },
                ProvisionalLine::Confirmed {
                    table: "team_notes".to_owned(),
                    key: NOTE_KEY.to_owned(),
                },
                ProvisionalLine::RolledBack {
                    table: "team_notes".to_owned(),
                    key: NOTE_KEY.to_owned(),
                    cause: "unauthorized".to_owned(),
                },
                ProvisionalLine::Refused {
                    table: "team_notes".to_owned(),
                    key: NOTE_KEY.to_owned(),
                },
            ]
        );
    }

    /// The page's other lines stand no provisional line, and a cause with
    /// words of its own stays whole.
    #[test]
    fn no_other_line_parses_and_the_cause_keeps_its_words() {
        let page = [
            "linked: bob/89abcdef via 10.42.0.7:4432",
            "note: provisional: applied team_notes x by bob",
            "provisional: applied team_notes",
            "provisional: applied team_notes a b by bob",
            "provisional: confirmed team_notes",
            "provisional: refused",
            "provisional: rolled back team_notes k",
            &format!(
                "provisional: rolled back team_notes {NOTE_KEY} \
                 because two devices settled different changes at the sequence"
            ),
        ]
        .join("\n");
        let lines = provisional_lines(&page);
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines,
            vec![ProvisionalLine::RolledBack {
                table: "team_notes".to_owned(),
                key: NOTE_KEY.to_owned(),
                cause: "two devices settled different changes at the sequence".to_owned(),
            }]
        );
    }

    /// The last `tidy:` line counts, the content pane's tidy lines do not.
    #[test]
    fn the_last_tidy_line_counts_and_the_content_line_does_not() {
        let page = [
            "content tidy: 1 file(s) evicted",
            "tidy: 2 provisional row(s) remain",
            "tidy: 0 provisional row(s) remain",
        ]
        .join("\n");
        assert_eq!(tidy_remaining(&page), Some(0));
        assert_eq!(tidy_remaining("content tidy: 3 file(s) evicted\n"), None);
        assert_eq!(tidy_remaining(&team_page()), Some(2));
    }

    /// Each team-note line stands its body, its author and, where the row is a
    /// peer-applied entry, the account it stands from, and a body with `by` in
    /// it keeps the author as the line's last field.
    #[test]
    fn each_note_line_stands_its_fields() {
        let lines = note_lines(&team_page());
        assert_eq!(
            lines,
            vec![
                NoteLine {
                    body: "head to the ridge".to_owned(),
                    author: "bob".to_owned(),
                    provisional_from: Some("bob@demo.dev".to_owned()),
                },
                NoteLine {
                    body: "pack the rope".to_owned(),
                    author: "alice".to_owned(),
                    provisional_from: None,
                },
            ]
        );
        let page = "note: the rope by the gate by alice (provisional from alice@demo.dev)\n\
             linked: bob/89abcdef via 10.42.0.7:4432";
        assert_eq!(
            note_lines(page),
            vec![NoteLine {
                body: "the rope by the gate".to_owned(),
                author: "alice".to_owned(),
                provisional_from: Some("alice@demo.dev".to_owned()),
            }]
        );
    }
}
