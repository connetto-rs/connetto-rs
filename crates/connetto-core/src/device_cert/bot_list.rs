//! The bot list a certificate carries, beside its attestation level (R77
//! decision 12).
//!
//! The server binds the enrolling session's list as `app.bot_list`, the
//! wildcard for a person and the list for a bot, and the extension carries
//! that same text.

/// The certificate extension carrying the bot list text, non-critical,
/// whose value is the DER `UTF8String` of the text.
///
/// It is the second arc under connetto's IANA Private Enterprise Number,
/// 67105, beside the attestation extension's first.
pub const BOT_LIST_EXTENSION: &[u64] = &[1, 3, 6, 1, 4, 1, 67105, 2];

/// The extension value, a DER `UTF8String` of `text`.
///
/// The text is a list a login resolved, which a deployment with many tables
/// takes past the short length's reach, so the length takes DER's long form,
/// in as few octets as it needs, from 128 bytes on.
#[must_use]
pub(crate) fn extension_value(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let len = bytes.len().to_be_bytes();
    let significant = &len[len.iter().take_while(|&&octet| octet == 0).count()..];
    let mut der = Vec::with_capacity(bytes.len() + 2 + significant.len());
    der.push(0x0c);
    match significant {
        [] => der.push(0),
        [short @ 0..=0x7f] => der.push(*short),
        long => {
            debug_assert!(long.len() <= 8, "a usize has at most eight octets");
            #[expect(
                clippy::cast_possible_truncation,
                reason = "a usize has at most eight octets, so the count fits the low seven bits"
            )]
            der.push(0x80 | long.len() as u8);
            der.extend_from_slice(long);
        }
    }
    der.extend_from_slice(bytes);
    der
}

/// The text an extension value carries, `None` for anything outside the
/// DER `UTF8String` form, a length in more octets than it needs included,
/// or not valid UTF-8.
#[must_use]
pub(crate) fn from_extension_value(der: &[u8]) -> Option<String> {
    let [0x0c, first, rest @ ..] = der else {
        return None;
    };
    let (len, content) = if *first < 0x80 {
        (usize::from(*first), rest)
    } else {
        let (octets, content) = rest.split_at_checked(usize::from(first & 0x7f))?;
        if octets.first().is_none_or(|&octet| octet == 0) {
            return None;
        }
        let len = octets.iter().try_fold(0_usize, |acc, &octet| {
            acc.checked_mul(256)?.checked_add(usize::from(octet))
        })?;
        if len < 0x80 {
            return None;
        }
        (len, content)
    };
    if content.len() != len {
        return None;
    }
    core::str::from_utf8(content).ok().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::{extension_value, from_extension_value};

    /// The text an extension value round-trips, at every length form.
    #[test]
    fn the_extension_value_round_trips() {
        let long = "orders:read".repeat(20);
        let longer = "orders:read,".repeat(6_000);
        for text in ["*".to_owned(), "orders:read".to_owned(), long, longer] {
            let der = extension_value(&text);
            assert_eq!(
                from_extension_value(&der).as_deref(),
                Some(text.as_str()),
                "the list round-trips"
            );
        }
    }

    /// A value outside the `UTF8String` form reads as absent.
    #[test]
    fn a_value_outside_its_form_reads_as_absent() {
        for der in [
            vec![0x13, 0x01, b'*'],             // an IA5String
            vec![0x0c, 0x02, 0xff, 0xfe],       // a UTF8String of invalid UTF-8
            vec![0x0c, 0x05, b'h', b'i'],       // a length past the content
            vec![],                             // no value at all
            vec![0x0c, 0x81, 0x01, b'*'],       // a long form the short one covers
            vec![0x0c, 0x82, 0x00, 0x01, b'*'], // a long form with a leading zero
            vec![0x0c, 0x80, b'*'],             // the indefinite form
        ] {
            assert_eq!(
                from_extension_value(&der),
                None,
                "the value reads as absent"
            );
        }
    }
}
