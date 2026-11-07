//! The team notes panel (R77 slice 8): the team the account's roster names,
//! a field that writes a note into it, the team's notes with every peer-applied
//! row marked provisional by its author's account, one line per applied,
//! confirmed, rolled back or refused peer write, and the tidy control that
//! runs the eviction pass.
//!
//! The panel is native only, since the provenance reads and the peer events
//! sit behind the client's `peer` feature, and stands beside the peer panel
//! like it, in the panes' look with its own colour. Its notes and events stay
//! plain lines, which the star walk reads off the page dumps the way it reads
//! the `linked:` ones.

use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{FaBroom, FaClockRotateLeft, FaPlus, FaUsers};

use crate::platform::native::SessionParts;
use connetto_client::KeyValue;
use connetto_demo_support as support;
use connetto_dioxus::use_live;

/// The event lines the panel keeps, newest last.
const MAX_EVENT_LINES: usize = 40;

/// Appends the lines to the panel's standing, keeping the newest
/// [`MAX_EVENT_LINES`].
#[cfg(feature = "peer")]
fn append_lines(lines: &mut Signal<Vec<String>>, added: Vec<String>) {
    let mut held = lines.write();
    held.extend(added);
    if held.len() > MAX_EVENT_LINES {
        let excess = held.len() - MAX_EVENT_LINES;
        held.drain(..excess);
    }
}

/// The team the picker names, filled with the first team `roster` names
/// while the picker stands empty. The effect reads the roster, so a roster
/// landing after the panel mounts fills the picker too.
#[cfg(feature = "peer")]
fn use_first_team(roster: ReadSignal<Vec<String>>) -> Signal<String> {
    let mut team: Signal<String> = use_signal(String::new);
    use_effect(move || {
        let first = roster.read().first().cloned();
        if let Some(first) = first
            && team.peek().is_empty()
        {
            team.set(first);
        }
    });
    team
}

/// The team notes panel (R77 slice 8): the team picker, the note's field and
/// write, the team's notes with the peer rows marked provisional, and one
/// line per R77 event.
#[cfg(feature = "peer")]
#[component]
pub(crate) fn TeamNotesPanel(parts: SessionParts) -> Element {
    let client = parts.0.client.clone();

    // The teams the account's roster names, the picker's source.
    let teams_live = use_live::<_, _, String>(&client, support::teams_of_account());
    let teams: Vec<String> = teams_live.value().read().iter().cloned().collect();
    let mut team = use_first_team(teams_live.value());

    // One line per R77 event, the panel's standing the run reads off the page.
    let events: Signal<Vec<String>> = use_signal(Vec::new);
    let event_rx = client.events();
    {
        let mut lines = events;
        use_hook(move || {
            spawn(async move {
                let mut rx = event_rx;
                while let Ok(event) = rx.recv().await {
                    let added = support::provisional_event_lines(&event);
                    if !added.is_empty() {
                        append_lines(&mut lines, added);
                    }
                }
            })
        });
    }

    // The tidy control's client, the pass this replica runs.
    let tidy_client = client.clone();
    let tidy_events = events;

    let picked = team.read().clone();
    let event_lines: Vec<String> = events.read().iter().cloned().collect();
    rsx! {
        div { class: "panel toned tone-team",
            h2 {
                Icon { icon: FaUsers, width: 14, height: 14 }
                "team notes "
                span { class: "badge synced", "synced + peer" }
            }
            p { {support::view::team_line(&picked)} }
            p { {support::view::teams_line(&teams)} }
            input {
                r#type: "text",
                name: "team",
                placeholder: "the team to read and write",
                value: picked.clone(),
                oninput: move |event| team.set(event.value()),
            }
            if !picked.is_empty() {
                TeamNotes {
                    key: "{picked}",
                    parts: parts.clone(),
                    team: picked.clone(),
                }
            }
            if !event_lines.is_empty() {
                h3 {
                    Icon { icon: FaClockRotateLeft, width: 12, height: 12 }
                    "peer activity"
                }
                div { class: "lines",
                    for line in event_lines {
                        p { {line} }
                    }
                }
            }
            div { class: "row",
                button {
                    onclick: move |_| {
                        let client = tidy_client.clone();
                        let mut lines = tidy_events;
                        spawn(async move {
                            match client.tidy().await {
                                Ok(()) => {
                                    let remaining = client
                                        .provisional_rows()
                                        .await
                                        .map(|rows| {
                                            rows.iter()
                                                .filter(|row| {
                                                    row.state
                                                        == connetto_client::provisional::ProvisionalState::Provisional
                                                })
                                                .count()
                                        })
                                        .unwrap_or_default();
                                    append_lines(&mut lines, vec![support::tidy_line(remaining)]);
                                }
                                Err(err) => {
                                    tracing::error!(error = %err, "the tidy pass failed");
                                    append_lines(
                                        &mut lines,
                                        vec![format!("tidy: the pass failed: {err}")],
                                    );
                                }
                            }
                        });
                    },
                    Icon { icon: FaBroom, width: 14, height: 14 }
                    "Tidy"
                }
            }
            if let Some(err) = teams_live.error().read().clone() {
                p { class: "error", "teams error: {err}" }
            }
            super::PaneInfo { text: support::TEAM_INFO }
        }
    }
}

/// One team's half of the panel: its notes with the peer rows marked
/// provisional, and the note's field and write.
#[cfg(feature = "peer")]
#[component]
fn TeamNotes(parts: SessionParts, team: String) -> Element {
    let client = parts.0.client.clone();
    let author = parts
        .0
        .native
        .session()
        .map(|session| session.user_id().to_owned());
    let notes_live =
        use_live::<_, _, support::TeamNote>(&client, support::team_notes_for(team.clone()));
    // The team's roster rows, which the replica settles a peer's note against.
    let members_live = use_live::<_, _, String>(&client, support::team_members_of(team.clone()));
    // The notes' lines, the peer rows marked provisional off the provenance.
    let note_lines: Signal<Vec<String>> = use_signal(Vec::new);
    {
        let marks_client = client.clone();
        let mut lines = note_lines;
        use_hook(move || {
            spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let rows: Vec<support::TeamNote> =
                        notes_live.value().peek().iter().cloned().collect();
                    let mut held = Vec::with_capacity(rows.len());
                    for row in &rows {
                        let key = vec![KeyValue::Blob(row.id.as_bytes().to_vec())];
                        let account = marks_client
                            .provenance("team_notes", &key)
                            .await
                            .ok()
                            .flatten()
                            .filter(|record| {
                                record.state
                                    == connetto_client::provisional::ProvisionalState::Provisional
                            })
                            .map(|record| record.account);
                        held.push(support::team_note_line(row, account.as_deref()));
                    }
                    if *lines.peek() != held {
                        lines.set(held);
                    }
                }
            })
        });
    }
    // The note's field and its last write's outcome.
    let mut note: Signal<String> = use_signal(String::new);
    let outcome: Signal<String> = use_signal(|| "idle".to_owned());
    let write_client = client.clone();
    let write_team = team.clone();
    let write_author = author.clone();
    let write_outcome = outcome;

    let notes_error = notes_live.error().read().clone();
    let members: Vec<String> = members_live.value().read().iter().cloned().collect();
    let lines: Vec<String> = note_lines.read().iter().cloned().collect();
    let outcome = outcome.read().clone();
    rsx! {
        p { {support::view::members_line(&members)} }
        div { class: "lines",
            if lines.is_empty() {
                p { class: "status", "no notes in this team yet" }
            }
            for line in lines {
                p { {line} }
            }
        }
        if let Some(err) = notes_error {
            p { class: "error", "notes error: {err}" }
        }
        div { class: "row",
            input {
                r#type: "text",
                name: "note-body",
                placeholder: "a note for the team",
                value: "{note}",
                oninput: move |event| note.set(event.value()),
            }
            button {
                class: "primary",
                onclick: move |_| {
                    let client = write_client.clone();
                    let team = write_team.clone();
                    let author = write_author.clone();
                    let mut note = note;
                    let mut outcome = write_outcome;
                    spawn(async move {
                        let body = note.peek().trim().to_owned();
                        if body.is_empty() {
                            outcome.set("the note needs some text".to_owned());
                            return;
                        }
                        match support::add_team_note(
                            &client,
                            &team,
                            author.as_deref().unwrap_or_default(),
                            &body,
                        )
                        .await
                        {
                            Ok(_) => {
                                note.set(String::new());
                                outcome.set("note added".to_owned());
                            }
                            Err(err) => outcome.set(format!("note add failed: {err}")),
                        }
                    });
                },
                Icon { icon: FaPlus, width: 14, height: 14 }
                "Add note"
            }
        }
        p { class: "status", {outcome} }
    }
}

#[cfg(all(test, feature = "peer"))]
mod tests {
    use std::cell::RefCell;
    use std::time::Duration;

    use dioxus::dioxus_core::NoOpMutations;
    use dioxus::prelude::*;

    use super::use_first_team;

    thread_local! {
        static ROSTER: RefCell<Option<Signal<Vec<String>>>> = const { RefCell::new(None) };
        static PICKED: RefCell<String> = const { RefCell::new(String::new()) };
    }

    fn picker() -> Element {
        let roster = use_signal(Vec::<String>::new);
        let team = use_first_team(roster.into());
        ROSTER.with(|slot| *slot.borrow_mut() = Some(roster));
        PICKED.with(|picked| *picked.borrow_mut() = team.read().clone());
        rsx! {}
    }

    /// Lets the dom run its effects and renders until it has no work left.
    async fn settle(dom: &mut VirtualDom) {
        for _ in 0..5 {
            if tokio::time::timeout(Duration::from_millis(50), dom.wait_for_work())
                .await
                .is_err()
            {
                break;
            }
            dom.render_immediate(&mut NoOpMutations);
        }
    }

    /// The roster lands after the panel mounts, as it does when the live
    /// query's first refresh comes back, and the picker still names its
    /// first team, which is what shows the note form.
    #[tokio::test]
    async fn a_roster_landing_after_mount_picks_its_first_team() {
        let mut dom = VirtualDom::new(picker);
        dom.rebuild_in_place();
        settle(&mut dom).await;
        assert_eq!(PICKED.with(|picked| picked.borrow().clone()), "");

        let mut roster = ROSTER.with(|slot| slot.borrow().expect("the picker rendered"));
        dom.in_runtime(|| roster.set(vec!["T1".to_owned(), "T2".to_owned()]));
        settle(&mut dom).await;
        assert_eq!(PICKED.with(|picked| picked.borrow().clone()), "T1");
    }

    /// A picked team stands when the roster changes afterwards, so a team
    /// the user typed is not overwritten either.
    #[tokio::test]
    async fn a_picked_team_stands_when_the_roster_changes() {
        let mut dom = VirtualDom::new(picker);
        dom.rebuild_in_place();
        let mut roster = ROSTER.with(|slot| slot.borrow().expect("the picker rendered"));
        dom.in_runtime(|| roster.set(vec!["T1".to_owned()]));
        settle(&mut dom).await;
        assert_eq!(PICKED.with(|picked| picked.borrow().clone()), "T1");

        dom.in_runtime(|| roster.set(vec!["T0".to_owned(), "T1".to_owned()]));
        settle(&mut dom).await;
        assert_eq!(PICKED.with(|picked| picked.borrow().clone()), "T1");
    }
}
