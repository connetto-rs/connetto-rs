//! The acknowledgements a deployment gives for the translation warnings that
//! refuse its build otherwise, like `#[expect]` for a lint.

use pg2sqlite::warnings::{DeclaredObject, DowngradeLocation, TranslationWarning};

/// Where pg2sqlite places a warning, each name the last identifier as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarningPlace {
    /// A column of a table.
    Column {
        /// The table declaring the column.
        table: String,
        /// The column.
        column: String,
    },
    /// An index on a table.
    Index {
        /// The indexed table.
        table: String,
        /// The index.
        index: String,
    },
    /// A table, holding what its declaration or a write into it casts.
    Table(String),
    /// A view, holding what its query or a write into it casts.
    View(String),
    /// A row level security policy, holding what the functions it calls cast.
    Policy {
        /// The table the policy is declared on.
        table: String,
        /// The policy.
        policy: String,
    },
    /// A trigger, holding what the function it executes casts.
    Trigger {
        /// The table or view the trigger fires on.
        table: String,
        /// The trigger.
        trigger: String,
    },
}

impl WarningPlace {
    /// `column` of `table`.
    pub fn column(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self::Column {
            table: table.into(),
            column: column.into(),
        }
    }

    /// `index` on `table`.
    pub fn index(table: impl Into<String>, index: impl Into<String>) -> Self {
        Self::Index {
            table: table.into(),
            index: index.into(),
        }
    }

    /// The table `name`.
    pub fn table(name: impl Into<String>) -> Self {
        Self::Table(name.into())
    }

    /// The view `name`.
    pub fn view(name: impl Into<String>) -> Self {
        Self::View(name.into())
    }

    /// `policy` on `table`.
    pub fn policy(table: impl Into<String>, policy: impl Into<String>) -> Self {
        Self::Policy {
            table: table.into(),
            policy: policy.into(),
        }
    }

    /// `trigger` on `table`.
    pub fn trigger(table: impl Into<String>, trigger: impl Into<String>) -> Self {
        Self::Trigger {
            table: table.into(),
            trigger: trigger.into(),
        }
    }
}

impl From<&DeclaredObject> for WarningPlace {
    fn from(object: &DeclaredObject) -> Self {
        match object {
            DeclaredObject::Table { name } => Self::table(name),
            DeclaredObject::View { name } => Self::view(name),
            DeclaredObject::Index { table, name } => Self::index(table, name),
            DeclaredObject::Policy { table, name } => Self::policy(table, name),
            DeclaredObject::Trigger { table, name } => Self::trigger(table, name),
        }
    }
}

impl std::fmt::Display for WarningPlace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column { table, column } => write!(f, "column {table}.{column}"),
            Self::Index { table, index } => write!(f, "index {index} on {table}"),
            Self::Table(name) => write!(f, "table {name}"),
            Self::View(name) => write!(f, "view {name}"),
            Self::Policy { table, policy } => write!(f, "policy {policy} on {table}"),
            Self::Trigger { table, trigger } => write!(f, "trigger {trigger} on {table}"),
        }
    }
}

/// What a warning does to the build.
enum Verdict {
    /// Printed and logged, never refused.
    Shown,
    /// Refused unless acknowledged at the place, `None` when it has none.
    Refuses(Option<WarningPlace>),
}

/// The verdict on `warning`, a kind connetto has not classified refusing.
fn verdict(warning: &TranslationWarning) -> Verdict {
    match warning {
        TranslationWarning::LossyDrop { .. }
        | TranslationWarning::LeftToServer { .. }
        | TranslationWarning::HiddenReferencedRows { .. }
        | TranslationWarning::ReadsLeftToServer { .. } => Verdict::Shown,
        TranslationWarning::LossyDowngrade { location, .. } => Verdict::Refuses(match location {
            DowngradeLocation::Column { table, column } => {
                Some(WarningPlace::column(table, column))
            }
            DowngradeLocation::Object(object)
            | DowngradeLocation::Expression {
                object: Some(object),
                ..
            } => Some(object.into()),
            DowngradeLocation::Expression { object: None, .. } | DowngradeLocation::Script => None,
        }),
        TranslationWarning::UnreadableRowKey { table, column } => {
            Verdict::Refuses(Some(WarningPlace::column(table, column)))
        }
        TranslationWarning::RlsDeniesEveryRow { table } => {
            Verdict::Refuses(Some(WarningPlace::table(table)))
        }
        _ => Verdict::Refuses(None),
    }
}

/// The warnings a deployment accepts, each by its place with a reason.
#[derive(Debug, Clone, Default)]
pub struct Acknowledgements {
    /// Each place with the reason it is accepted, in the order given.
    entries: Vec<(WarningPlace, String)>,
}

impl Acknowledgements {
    /// None, so every refusing warning refuses the build.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Accept every refusing warning at `place`, for `reason`.
    #[must_use]
    pub fn with(mut self, place: WarningPlace, reason: impl Into<String>) -> Self {
        self.entries.push((place, reason.into()));
        self
    }

    /// The reason accepting `warning`, `None` when it is shown only or no
    /// acknowledgement names its place.
    pub(crate) fn reason_for(&self, warning: &TranslationWarning) -> Option<&str> {
        let Verdict::Refuses(Some(place)) = verdict(warning) else {
            return None;
        };
        self.reason_at(&place)
    }

    /// The reason the deployment gives for a warning at `place`.
    fn reason_at(&self, place: &WarningPlace) -> Option<&str> {
        self.entries
            .iter()
            .find(|(acknowledged, _)| acknowledged == place)
            .map(|(_, reason)| reason.as_str())
    }

    /// Refuse `warnings` when one of them refuses without an acknowledgement,
    /// or when an acknowledgement matches none of them.
    ///
    /// # Errors
    ///
    /// [`Unacknowledged`] naming both.
    pub(crate) fn check(&self, warnings: &[TranslationWarning]) -> Result<(), Unacknowledged> {
        let places: Vec<(&TranslationWarning, Option<WarningPlace>)> = warnings
            .iter()
            .filter_map(|warning| match verdict(warning) {
                Verdict::Shown => None,
                Verdict::Refuses(place) => Some((warning, place)),
            })
            .collect();
        let unacknowledged: Vec<TranslationWarning> = places
            .iter()
            .filter(|(_, place)| {
                place
                    .as_ref()
                    .is_none_or(|place| self.reason_at(place).is_none())
            })
            .map(|(warning, _)| (*warning).clone())
            .collect();
        let unfulfilled: Vec<WarningPlace> = self
            .entries
            .iter()
            .filter(|(acknowledged, _)| {
                !places
                    .iter()
                    .any(|(_, place)| place.as_ref() == Some(acknowledged))
            })
            .map(|(acknowledged, _)| acknowledged.clone())
            .collect();
        if unacknowledged.is_empty() && unfulfilled.is_empty() {
            Ok(())
        } else {
            Err(Unacknowledged {
                unacknowledged,
                unfulfilled,
            })
        }
    }
}

/// The warnings a build refuses and the acknowledgements matching none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unacknowledged {
    /// Each refusing warning no acknowledgement names, in translation order.
    pub unacknowledged: Vec<TranslationWarning>,
    /// Each acknowledgement whose place holds no refusing warning.
    pub unfulfilled: Vec<WarningPlace>,
}

impl std::fmt::Display for Unacknowledged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the build refuses")?;
        for warning in &self.unacknowledged {
            match verdict(warning) {
                Verdict::Refuses(Some(place)) => {
                    write!(f, "\n- {warning}, unless {place} is acknowledged")?;
                }
                Verdict::Refuses(None) | Verdict::Shown => {
                    write!(f, "\n- {warning}, which has no place to acknowledge")?;
                }
            }
        }
        for place in &self.unfulfilled {
            write!(
                f,
                "\n- the acknowledgement of {place}, which no warning holds"
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for Unacknowledged {}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `CHAR(2)` column downgraded on `notes.code`.
    fn column_downgrade() -> TranslationWarning {
        TranslationWarning::LossyDowngrade {
            construct: "CHAR".to_owned(),
            from: "CHAR(2)".to_owned(),
            to: "TEXT".to_owned(),
            location: DowngradeLocation::Column {
                table: "notes".to_owned(),
                column: "code".to_owned(),
            },
            reason: "no longer blank padded".to_owned(),
        }
    }

    /// A text-to-timestamp cast held by `object`, `None` for a top-level query.
    fn cast_in(object: Option<DeclaredObject>) -> TranslationWarning {
        TranslationWarning::LossyDowngrade {
            construct: "CAST".to_owned(),
            from: "PostgreSQL date and time input".to_owned(),
            to: "SQLite's date functions".to_owned(),
            location: DowngradeLocation::Expression {
                object,
                text: "CAST(title AS TIMESTAMP)".to_owned(),
            },
            reason: "read as ISO 8601".to_owned(),
        }
    }

    #[test]
    fn an_unacknowledged_downgrade_refuses_the_build() {
        let refused = Acknowledgements::new()
            .check(&[column_downgrade()])
            .expect_err("nothing acknowledges the column");
        assert_eq!(refused.unacknowledged, [column_downgrade()]);
        assert_eq!(refused.unfulfilled, []);
    }

    #[test]
    fn an_acknowledgement_at_the_place_admits_the_warning_with_its_reason() {
        let acknowledged = Acknowledgements::new().with(
            WarningPlace::column("notes", "code"),
            "codes are always two letters",
        );
        acknowledged
            .check(&[column_downgrade()])
            .expect("the column is acknowledged");
        assert_eq!(
            acknowledged.reason_for(&column_downgrade()),
            Some("codes are always two letters")
        );
    }

    #[test]
    fn an_acknowledgement_at_another_place_refuses_both() {
        let refused = Acknowledgements::new()
            .with(WarningPlace::column("notes", "created"), "unused")
            .check(&[column_downgrade()])
            .expect_err("the acknowledgement names another column");
        assert_eq!(refused.unacknowledged, [column_downgrade()]);
        assert_eq!(
            refused.unfulfilled,
            [WarningPlace::column("notes", "created")]
        );
    }

    #[test]
    fn a_cast_counts_as_the_object_holding_it() {
        let in_policy = cast_in(Some(DeclaredObject::Policy {
            table: "notes".to_owned(),
            name: "notes_p".to_owned(),
        }));
        let in_write = cast_in(Some(DeclaredObject::Table {
            name: "log".to_owned(),
        }));
        Acknowledgements::new()
            .with(
                WarningPlace::policy("notes", "notes_p"),
                "policy reads ISO text",
            )
            .with(WarningPlace::table("log"), "the backfill writes ISO text")
            .check(&[in_policy.clone(), in_policy, in_write])
            .expect("one acknowledgement covers every cast at its place");
    }

    #[test]
    fn a_cast_with_no_place_refuses_whatever_is_acknowledged() {
        let refused = Acknowledgements::new()
            .with(WarningPlace::table("notes"), "unused")
            .check(&[cast_in(None)])
            .expect_err("a top-level query has no place");
        assert_eq!(refused.unacknowledged, [cast_in(None)]);
        assert!(refused.to_string().contains("no place to acknowledge"));
    }

    #[test]
    fn what_the_replica_leaves_to_the_server_never_refuses() {
        let left = TranslationWarning::LeftToServer {
            construct: "TRIGGER".to_owned(),
            name: "audit".to_owned(),
            table: "notes".to_owned(),
            reason: "the server writes the audit row".to_owned(),
        };
        Acknowledgements::new()
            .check(std::slice::from_ref(&left))
            .expect("shown only");
        let refused = Acknowledgements::new()
            .with(WarningPlace::trigger("notes", "audit"), "unused")
            .check(&[left])
            .expect_err("a shown warning fulfils no acknowledgement");
        assert_eq!(
            refused.unfulfilled,
            [WarningPlace::trigger("notes", "audit")]
        );
    }

    #[test]
    fn an_unreadable_key_and_a_denying_table_refuse_at_their_places() {
        let warnings = [
            TranslationWarning::UnreadableRowKey {
                table: "notes".to_owned(),
                column: "id".to_owned(),
            },
            TranslationWarning::RlsDeniesEveryRow {
                table: "drafts".to_owned(),
            },
        ];
        Acknowledgements::new()
            .check(&warnings)
            .expect_err("both refuse unacknowledged");
        Acknowledgements::new()
            .with(WarningPlace::column("notes", "id"), "notes are append-only")
            .with(WarningPlace::table("drafts"), "drafts stay on the server")
            .check(&warnings)
            .expect("both acknowledged");
    }
}
