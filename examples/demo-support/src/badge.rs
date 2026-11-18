//! The account badge, one colour and one animal derived from an account id,
//! which every frontend draws wherever an account shows.

use serde::{Deserialize, Serialize};

/// The Font Awesome Free solid animals a badge draws, in derivation order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Animal {
    /// `cat`.
    Cat,
    /// `cow`.
    Cow,
    /// `crow`.
    Crow,
    /// `dog`.
    Dog,
    /// `dove`.
    Dove,
    /// `dragon`.
    Dragon,
    /// `fish`.
    Fish,
    /// `frog`.
    Frog,
    /// `hippo`.
    Hippo,
    /// `horse`.
    Horse,
    /// `kiwi-bird`.
    KiwiBird,
    /// `otter`.
    Otter,
    /// `shrimp`.
    Shrimp,
    /// `spider`.
    Spider,
    /// `worm`.
    Worm,
    /// `locust`.
    Locust,
}

impl Animal {
    /// Every animal, the list [`AccountBadge::of`] indexes.
    pub const ALL: [Self; 16] = [
        Self::Cat,
        Self::Cow,
        Self::Crow,
        Self::Dog,
        Self::Dove,
        Self::Dragon,
        Self::Fish,
        Self::Frog,
        Self::Hippo,
        Self::Horse,
        Self::KiwiBird,
        Self::Otter,
        Self::Shrimp,
        Self::Spider,
        Self::Worm,
        Self::Locust,
    ];

    /// The Font Awesome icon name, which the frontends' glyph tables key on.
    #[must_use]
    pub const fn icon_name(self) -> &'static str {
        match self {
            Self::Cat => "cat",
            Self::Cow => "cow",
            Self::Crow => "crow",
            Self::Dog => "dog",
            Self::Dove => "dove",
            Self::Dragon => "dragon",
            Self::Fish => "fish",
            Self::Frog => "frog",
            Self::Hippo => "hippo",
            Self::Horse => "horse",
            Self::KiwiBird => "kiwi-bird",
            Self::Otter => "otter",
            Self::Shrimp => "shrimp",
            Self::Spider => "spider",
            Self::Worm => "worm",
            Self::Locust => "locust",
        }
    }
}

/// The hues a badge takes, in degrees, spaced so that neighbours differ at
/// a glance. The stylesheet fixes saturation and lightness per colour scheme,
/// so each hue reads on a light and on a dark background.
pub const HUES: [u16; 12] = [0, 30, 50, 90, 140, 170, 195, 215, 245, 275, 300, 330];

/// One account's badge, a pure function of its id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountBadge {
    /// The account id.
    pub id: String,
    /// The id's first segment, [`crate::short_id`].
    pub short: String,
    /// The badge's hue in degrees, one of [`HUES`].
    pub hue: u16,
    /// The badge's animal.
    pub animal: Animal,
}

impl AccountBadge {
    /// The badge of the account `id`. A UUID's sixteen bytes pick the hue
    /// and the animal, whatever the case of its hex digits, and any other id
    /// its UTF-8 bytes.
    #[must_use]
    pub fn of(id: &str) -> Self {
        let digest = match uuid_bytes(id) {
            Some(bytes) => fnv1a(&bytes),
            None => fnv1a(id.as_bytes()),
        };
        Self {
            id: id.to_owned(),
            short: crate::short_id(id).to_owned(),
            hue: HUES[index(digest >> 32, HUES.len())],
            animal: Animal::ALL[index(digest, Animal::ALL.len())],
        }
    }
}

/// `digest` reduced to an index below `len`.
fn index(digest: u64, len: usize) -> usize {
    // Both lists are far shorter than `u64::MAX`, and the remainder is below
    // `len`, so both conversions are exact.
    (digest % len as u64) as usize
}

/// The sixteen bytes of a UUID in its hyphenated or plain hex form.
fn uuid_bytes(id: &str) -> Option<[u8; 16]> {
    let mut digits = id.bytes().filter(|byte| *byte != b'-');
    let mut bytes = [0_u8; 16];
    for byte in &mut bytes {
        let high = hex_value(digits.next()?)?;
        let low = hex_value(digits.next()?)?;
        *byte = (high << 4) | low;
    }
    digits.next().is_none().then_some(bytes)
}

fn hex_value(digit: u8) -> Option<u8> {
    char::from(digit)
        .to_digit(16)
        .and_then(|value| u8::try_from(value).ok())
}

/// The 64-bit FNV-1a digest of `bytes`.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |digest, byte| {
        (digest ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::{AccountBadge, Animal};

    /// The derivation stays fixed, so an account keeps its badge across
    /// releases and devices. A change to the lists or the digest fails here.
    #[test]
    fn a_badge_stays_the_same_for_the_same_id() {
        for (id, hue, animal) in [
            ("3f2a9c1e-7b4d-5e21-9a0c-2d4e6f8a1b3c", 215, Animal::Dove),
            ("9c1e3f2a-0b4d-5e21-8a0c-2d4e6f8a1b3d", 170, Animal::Dog),
            ("00000000-0000-0000-0000-000000000000", 275, Animal::Dragon),
            ("bob@demo.dev", 275, Animal::Fish),
        ] {
            let badge = AccountBadge::of(id);
            assert_eq!((badge.hue, badge.animal), (hue, animal), "{id}");
        }
        let badge = AccountBadge::of("3f2a9c1e-7b4d-5e21-9a0c-2d4e6f8a1b3c");
        assert_eq!(badge.short, "3f2a9c1e");
    }

    /// A UUID gets one badge however its hex digits are written.
    #[test]
    fn a_uuid_gets_one_badge_in_any_spelling() {
        let lower = AccountBadge::of("3f2a9c1e-7b4d-5e21-9a0c-2d4e6f8a1b3c");
        for spelling in [
            "3F2A9C1E-7B4D-5E21-9A0C-2D4E6F8A1B3C",
            "3f2a9c1e7b4d5e219a0c2d4e6f8a1b3c",
        ] {
            let badge = AccountBadge::of(spelling);
            assert_eq!((badge.hue, badge.animal), (lower.hue, lower.animal));
        }
    }
}
