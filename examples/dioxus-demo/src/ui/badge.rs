//! Account badges and the signed-in screen's name subscription.

use connetto_demo_support as support;
use connetto_dioxus::use_live;
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{
    FaCat, FaCow, FaCrow, FaDog, FaDove, FaDragon, FaFish, FaFrog, FaHippo, FaHorse, FaKiwiBird,
    FaLocust, FaOtter, FaShrimp, FaSpider, FaWorm,
};

#[derive(Clone, Copy)]
pub(super) struct Names(ReadSignal<Vec<support::User>>);

#[derive(Clone, Copy)]
pub(super) struct StoredNames(pub Signal<support::view::StoredNames>);

/// Follows names once for the signed-in component tree.
#[component]
pub(super) fn NamedScreen(children: Element) -> Element {
    let super::ClientSlot(client) = use_context::<super::ClientSlot>();
    let super::UserId(identity) = use_context::<super::UserId>();
    let StoredNames(mut stored) = use_context::<StoredNames>();
    let client = client.read().as_ref().cloned().expect("signed-in client");
    let names = use_live(&client, support::UserNames).value();
    use_context_provider(|| Names(names));
    use_effect(move || {
        let identity = identity.read();
        let rows = names.read();
        if let Some(row) = rows.iter().find(|row| Some(&row.id) == identity.as_ref())
            && stored.peek().name(&row.id) != Some(row.login_name.as_str())
        {
            stored.write().remember(&row.id, &row.login_name);
            crate::platform::save_badge_names(&stored.read());
        }
    });
    children
}

/// Chooses a synced name before a stored account's name.
fn preferred_name<'name>(
    account: &str,
    rows: &'name [support::User],
    stored: &'name support::view::StoredNames,
) -> Option<&'name str> {
    rows.iter()
        .find(|row| row.id == account)
        .map(|row| row.login_name.as_str())
        .or_else(|| stored.name(account))
}

/// Draws an account's animal, preferred name and short identifier.
#[component]
pub(super) fn Badge(account: String) -> Element {
    let names = try_use_context::<Names>();
    let StoredNames(stored) = use_context::<StoredNames>();
    let rows = names.as_ref().map(|Names(rows)| rows.read());
    let name = preferred_name(
        &account,
        rows.as_deref().map_or(&[], Vec::as_slice),
        &stored.read(),
    )
    .map(str::to_owned);
    let badge = support::badge::AccountBadge::of(&account);
    rsx! {
        span {
            class: "badge",
            "data-account": account,
            style: "--badge-hue: {badge.hue}",
            {animal_icon(badge.animal)}
            if let Some(name) = name {
                span { class: "badge-name", "{name}" }
                " "
            }
            span { class: "badge-short", "{badge.short}" }
        }
    }
}

/// Selects the Font Awesome glyph for each badge animal.
fn animal_icon(animal: support::badge::Animal) -> Element {
    use support::badge::Animal;
    match animal {
        Animal::Cat => rsx! { Icon { icon: FaCat, width: 14, height: 14 } },
        Animal::Cow => rsx! { Icon { icon: FaCow, width: 14, height: 14 } },
        Animal::Crow => rsx! { Icon { icon: FaCrow, width: 14, height: 14 } },
        Animal::Dog => rsx! { Icon { icon: FaDog, width: 14, height: 14 } },
        Animal::Dove => rsx! { Icon { icon: FaDove, width: 14, height: 14 } },
        Animal::Dragon => rsx! { Icon { icon: FaDragon, width: 14, height: 14 } },
        Animal::Fish => rsx! { Icon { icon: FaFish, width: 14, height: 14 } },
        Animal::Frog => rsx! { Icon { icon: FaFrog, width: 14, height: 14 } },
        Animal::Hippo => rsx! { Icon { icon: FaHippo, width: 14, height: 14 } },
        Animal::Horse => rsx! { Icon { icon: FaHorse, width: 14, height: 14 } },
        Animal::KiwiBird => rsx! { Icon { icon: FaKiwiBird, width: 14, height: 14 } },
        Animal::Otter => rsx! { Icon { icon: FaOtter, width: 14, height: 14 } },
        Animal::Shrimp => rsx! { Icon { icon: FaShrimp, width: 14, height: 14 } },
        Animal::Spider => rsx! { Icon { icon: FaSpider, width: 14, height: 14 } },
        Animal::Worm => rsx! { Icon { icon: FaWorm, width: 14, height: 14 } },
        Animal::Locust => rsx! { Icon { icon: FaLocust, width: 14, height: 14 } },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synced_name_precedes_stored_name() {
        let mut stored = support::view::StoredNames::default();
        stored.remember("alice", "stored");
        let rows = vec![support::User {
            id: "alice".to_owned(),
            login_name: "synced".to_owned(),
        }];
        assert_eq!(preferred_name("alice", &rows, &stored), Some("synced"));
        assert_eq!(preferred_name("alice", &[], &stored), Some("stored"));
        assert_eq!(preferred_name("bob", &rows, &stored), None);
    }

    #[test]
    fn every_animal_has_a_glyph() {
        for animal in support::badge::Animal::ALL {
            assert!(animal_icon(animal).is_ok());
        }
    }
}
