//! The two-phone peer proof's phone-driving steps (R76), which read a host's
//! offer off its page and the page's own identity, and wait for the link's
//! end.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::time::{Instant, sleep};

use crate::inspector::PageSession;
use crate::peer_proof::{HotspotOfferLines, parse_offer_lines};

/// How long both pages get to drop their linked lines after a leave.
pub const LINK_END_BOUND: Duration = Duration::from_secs(120);

/// The offer a hosting phone shows, its lines read off the page, bounded.
///
/// # Errors
///
/// When the page does not show the whole offer within its bound, or does not
/// answer.
pub async fn read_hotspot_offer(app: &mut PageSession) -> Result<HotspotOfferLines> {
    app.wait_for_text("hotspot ssid: ", Duration::from_secs(45))
        .await
        .context("the host never showed its offer")?;
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let page = app.page_text().await?;
        if let Some(offer) = parse_offer_lines(&page) {
            return Ok(offer);
        }
        if Instant::now() >= deadline {
            bail!("the offer's lines never finished showing, the page shows:\n{page}");
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// The page's own peer identity, `badge/key prefix`, from its listening line.
///
/// # Errors
///
/// When the page names no peer identity within its bound, or does not
/// answer.
pub async fn own_identity(app: &mut PageSession) -> Result<String> {
    // The peer panel updates the listening line on its own poll, which the
    // device label does not wait for, so the wait polls the line itself.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let page = app.page_text().await?;
        if let Some(identity) = listening_identity(&page) {
            return Ok(identity.to_owned());
        }
        if Instant::now() >= deadline {
            bail!("the page names no peer identity");
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Wait for both pages to drop their linked lines, which the leave and stop
/// end the link.
///
/// # Errors
///
/// When a link line stays past [`LINK_END_BOUND`], or a page does not answer.
pub async fn wait_link_ends(app: &mut PageSession, peer_app: &mut PageSession) -> Result<()> {
    let deadline = Instant::now() + LINK_END_BOUND;
    loop {
        let host_page = app.page_text().await?;
        let peer_page = peer_app.page_text().await?;
        if !host_page.contains("linked: ") && !peer_page.contains("linked: ") {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("the link did not end after the phones left the hotspot");
        }
        sleep(Duration::from_secs(2)).await;
    }
}

/// Extract the complete badge and key prefix from a listening line.
fn listening_identity(page: &str) -> Option<&str> {
    page.lines().find_map(|line| {
        line.trim()
            .strip_prefix("peer: listening on ")?
            .split_once(" as ")
            .map(|(_, identity)| identity.trim())
    })
}

/// Compare peer badges by short account id and key prefix.
pub fn same_peer(first: &str, second: &str) -> bool {
    fn parts(identity: &str) -> Option<(&str, &str)> {
        let (badge, key) = identity.trim().rsplit_once('/')?;
        let account = badge.split_whitespace().last()?.split('-').next()?;
        (!account.is_empty() && !key.is_empty()).then_some((account, key))
    }
    matches!((parts(first), parts(second)), (Some(first), Some(second)) if first == second)
}

/// Whether a linked line names the peer, independently of its display name.
pub fn page_names_peer(page: &str, peer: &str) -> bool {
    page.lines().any(|line| {
        line.trim()
            .strip_prefix("linked: ")
            .is_some_and(|identity| {
                let identity = identity
                    .rsplit_once(" via ")
                    .map_or(identity, |(badge, _)| badge);
                same_peer(identity, peer)
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listening_identity_keeps_the_complete_badge() {
        assert_eq!(
            listening_identity("peer: listening on 4431 as alice d5e24135/01234567"),
            Some("alice d5e24135/01234567")
        );
        assert_eq!(
            listening_identity("peer: listening on 4431 as d5e24135/01234567"),
            Some("d5e24135/01234567")
        );
        assert_eq!(listening_identity("peer: not serving"), None);
    }

    #[test]
    fn peer_matching_uses_short_account_and_key() {
        assert!(same_peer("bob 89abcdef/01234567", "89abcdef-1234/01234567"));
        assert!(same_peer(
            "renamed bob 89abcdef/01234567",
            "bob 89abcdef/01234567"
        ));
        assert!(!same_peer("bob 89abcdef/01234567", "89abcdef/76543210"));
        assert!(!same_peer("bob 89abcdef/01234567", "d5e24135/01234567"));
        assert!(!same_peer("", ""));
        assert!(page_names_peer(
            "linked: bob 89abcdef/01234567 via 10.42.0.7:4431",
            "89abcdef/01234567"
        ));
        assert!(page_names_peer(
            "linked: 89abcdef/01234567",
            "bob 89abcdef/01234567"
        ));
        assert!(!page_names_peer(
            "peer: listening on 4431 as bob 89abcdef/01234567",
            "89abcdef/01234567"
        ));
    }
}
