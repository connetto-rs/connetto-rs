use std::collections::BTreeSet;
use std::sync::Arc;

use rls2fga::generator::tuple_generator::TupleQuery;
use rls2fga::types::{PolicyReads, RecordDescription, RelationShapes, RowNaming, TableId};
use sql_traits::prelude::{DatabaseLike, FunctionLike, PolicyLike, TableLike};

use super::{Enumeration, GrantReach, ParserDB, Postgres, SetupError, Shapes, Translated};

/// A live translation and the tables it cannot serve.
pub struct LiveTranslation {
    /// The rules for the served catalog.
    pub translated: Translated,
    /// Refused tables, including their transitive policy readers.
    pub refused: Vec<String>,
}

/// One table's contribution to the translation.
#[derive(serde::Serialize)]
struct TranslationShare<'translation> {
    /// Relations naming or reading this table.
    relations: Vec<&'translation RelationShapes>,
    /// Row identities assigned to this table.
    naming: Vec<&'translation RowNaming>,
    /// Tuple queries naming or reading this table.
    queries: Vec<&'translation TupleQuery>,
}

impl Translated {
    /// Translate live rules against the served catalog's numbering.
    ///
    /// # Errors
    ///
    /// Returns [`SetupError::Unplannable`] when no translation can be produced.
    pub fn follow(
        &self,
        database: &crate::rules::LiveDatabase,
        changed: &[String],
    ) -> Result<LiveTranslation, SetupError> {
        self.follow_database(database, changed)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the ordered refusal checks share the live translation and accepted outputs"
    )]
    fn follow_database<Database: DatabaseLike>(
        &self,
        database: &Database,
        changed: &[String],
    ) -> Result<LiveTranslation, SetupError> {
        let translation = self
            .translator
            .translate(database)
            .map_err(|error| SetupError::Unplannable(error.to_string()))?;
        let mut refused = BTreeSet::new();
        for note in translation.notes() {
            if note.severity().diverges_from_database() {
                refused.extend(subject_tables(database, &note.subject(), changed));
            }
        }
        let outputs = Arc::new(translation.outputs_accepting_gaps());
        let translation = outputs.translation();
        let catalog = self.shapes.catalog();
        let served: BTreeSet<&str> = catalog.tables().map(TableLike::table_name).collect();
        let policy_reads = translation.policy_reads().to_vec();
        for policy in &policy_reads {
            if !policy.unresolved.is_empty()
                || policy
                    .reads
                    .iter()
                    .any(|table| !served.contains(table.name()))
            {
                refused.insert(policy.table.name().to_owned());
            }
        }
        let enumerations: Vec<_> = outputs
            .tuple_queries()
            .iter()
            .filter_map(Enumeration::of)
            .collect();
        for relation in translation.relations() {
            if relation
                .shapes
                .iter()
                .any(|shape| !shape_known(catalog, shape))
            {
                let mut reached = false;
                for naming in translation
                    .row_naming()
                    .iter()
                    .filter(|naming| naming.type_name == relation.type_name)
                {
                    refused.insert(naming.table.name().to_owned());
                    reached = true;
                }
                if !reached {
                    refused.extend(changed.iter().cloned());
                }
            }
        }
        let shapes = Arc::new(
            Shapes::new::<Postgres>(catalog.clone(), translation.relations(), &enumerations)
                .with_row_naming(translation.row_naming())
                .with_resulting_rows(translation.resulting_rows())
                .with_action_relations(translation.action_relations())
                .with_required_parameters(translation.notes()),
        );
        for gap in shapes.uncovered() {
            refused.extend(subject_tables(database, &gap.table, changed));
            refused.extend(
                translation
                    .row_naming()
                    .iter()
                    .filter(|naming| naming.type_name == gap.type_name)
                    .map(|naming| naming.table.name().to_owned()),
            );
        }
        let model = outputs.json_model();
        let mut answers = Vec::new();
        for answer in translation.action_relations() {
            if GrantReach::of(
                &model,
                translation.row_naming(),
                std::slice::from_ref(answer),
            )
            .is_err()
            {
                let reached: Vec<_> = translation
                    .row_naming()
                    .iter()
                    .filter(|naming| naming.type_name == answer.type_name)
                    .map(|naming| naming.table.name().to_owned())
                    .collect();
                if reached.is_empty() {
                    refused.extend(changed.iter().cloned());
                } else {
                    refused.extend(reached);
                }
            } else {
                answers.push(answer.clone());
            }
        }
        let index_blocked = index_refusals(&shapes, translation, changed, &mut refused);
        refuse_readers(&mut refused, &policy_reads);
        let reach = Arc::new(GrantReach::of(&model, translation.row_naming(), &answers)?);
        let blocked_types: BTreeSet<_> = translation
            .row_naming()
            .iter()
            .filter(|naming| refused.contains(naming.table.name()))
            .map(|naming| &naming.type_name)
            .collect();
        let blocked_relations: Vec<_> = translation
            .relations()
            .iter()
            .filter(|relation| {
                index_blocked.contains(relation)
                    || blocked_types.contains(&relation.type_name)
                    || relation.shapes.iter().any(|shape| {
                        shape.tables.iter().any(|table| {
                            refused.contains(table.name()) || !served.contains(table.name())
                        })
                    })
            })
            .collect();
        let tuples: Vec<_> = outputs
            .tuple_queries()
            .iter()
            .filter(|query| {
                query.skipped.is_none()
                    && query.description.as_ref().is_none_or(|description| {
                        !blocked_relations
                            .iter()
                            .any(|relation| relation.shapes.contains(description))
                            && description.tables.iter().all(|table| {
                                served.contains(table.name()) && !refused.contains(table.name())
                            })
                    })
            })
            .cloned()
            .collect();
        let shapes = if blocked_relations.is_empty() {
            shapes
        } else {
            let relations: Vec<_> = translation
                .relations()
                .iter()
                .filter(|relation| !blocked_relations.contains(relation))
                .cloned()
                .collect();
            let enumerations: Vec<_> = tuples.iter().filter_map(Enumeration::of).collect();
            Arc::new(
                Shapes::new::<Postgres>(catalog.clone(), &relations, &enumerations)
                    .with_row_naming(translation.row_naming())
                    .with_resulting_rows(translation.resulting_rows())
                    .with_action_relations(&answers)
                    .with_required_parameters(translation.notes()),
            )
        };
        let subjects = Arc::new((self.resolve_subjects)(
            &shapes,
            translation.relations(),
            &self.reader_role,
        ));
        let translated = Self {
            shapes,
            subjects,
            translator: self.translator.clone(),
            model,
            tuples,
            policy_tables: super::policy_tables(&outputs)
                .into_iter()
                .filter(|table| served.contains(table.as_str()) && !refused.contains(table))
                .collect(),
            reach,
            policy_reads,
            outputs,
            resolve_subjects: self.resolve_subjects,
            reader_role: self.reader_role.clone(),
        };
        refused.retain(|table| served.contains(table.as_str()));
        Ok(LiveTranslation {
            translated,
            refused: refused.into_iter().collect(),
        })
    }

    /// The digest of each served table's share of the translation, which a later
    /// process compares with its own to find the tables whose rules changed meanwhile.
    ///
    /// # Errors
    ///
    /// Returns [`SetupError::Unplannable`] when a share does not serialize.
    pub fn fingerprints(&self) -> Result<std::collections::BTreeMap<String, String>, SetupError> {
        self.translation_shares()
            .into_iter()
            .map(|(table, share)| {
                fingerprint(&share)
                    .map(|digest| (table, digest))
                    .map_err(|error| SetupError::Unplannable(error.to_string()))
            })
            .collect()
    }

    /// Select each served table's upstream fingerprint inputs.
    fn translation_shares(&self) -> std::collections::BTreeMap<String, TranslationShare<'_>> {
        let translation = self.outputs.translation();
        self.shapes
            .catalog()
            .tables()
            .map(|table| {
                let name = table.table_name();
                let naming: Vec<_> = translation
                    .row_naming()
                    .iter()
                    .filter(|naming| naming.table.name() == name)
                    .collect();
                let relations: Vec<_> = translation
                    .relations()
                    .iter()
                    .filter(|relation| {
                        naming
                            .iter()
                            .any(|naming| naming.type_name == relation.type_name)
                            || relation
                                .shapes
                                .iter()
                                .any(|shape| shape.tables.iter().any(|table| table.name() == name))
                    })
                    .collect();
                let queries = self
                    .outputs
                    .tuple_queries()
                    .iter()
                    .filter(|query| {
                        query.description.as_ref().is_some_and(|description| {
                            description.tables.iter().any(|table| table.name() == name)
                                || relations
                                    .iter()
                                    .any(|relation| relation.shapes.contains(description))
                        })
                    })
                    .collect();
                (
                    name.to_owned(),
                    TranslationShare {
                        relations,
                        naming,
                        queries,
                    },
                )
            })
            .collect()
    }
}

fn shape_known(catalog: &ParserDB, shape: &RecordDescription) -> bool {
    shape.tables.iter().all(|table| {
        catalog.tables().any(|candidate| {
            candidate.table_name() == table.name()
                && candidate.table_schema().unwrap_or("public")
                    == table.schema().unwrap_or("public")
        })
    })
}

fn index_refusals<'translation>(
    shapes: &Shapes<ParserDB>,
    translation: &'translation rls2fga::translator::Translation,
    changed: &[String],
    refused: &mut BTreeSet<String>,
) -> Vec<&'translation RelationShapes> {
    if !shapes.has_request_gated_recipe() || !shapes.required_parameters().is_empty() {
        return Vec::new();
    }
    let blocked: Vec<_> = translation
        .relations()
        .iter()
        .filter(|relation| {
            Shapes::new::<Postgres>(
                shapes.catalog().clone(),
                std::slice::from_ref(*relation),
                &[],
            )
            .has_request_gated_recipe()
        })
        .collect();
    for relation in &blocked {
        let mut reached = false;
        for naming in translation
            .row_naming()
            .iter()
            .filter(|naming| naming.type_name == relation.type_name)
        {
            refused.insert(naming.table.name().to_owned());
            reached = true;
        }
        if !reached {
            refused.extend(changed.iter().cloned());
        }
    }
    blocked
}

fn subject_tables<Database: DatabaseLike>(
    database: &Database,
    subject: &str,
    changed: &[String],
) -> BTreeSet<String> {
    let mut affected = BTreeSet::new();
    for table in database.tables() {
        let identity = TableId::from_stored(
            table.table_schema().map(str::to_owned),
            table.table_name().to_owned(),
        );
        if table.table_name() == subject || identity.to_string() == subject {
            affected.insert(table.table_name().to_owned());
        }
        if table
            .policies(database)
            .is_ok_and(|mut policies| policies.any(|policy| policy.name() == subject))
        {
            affected.insert(table.table_name().to_owned());
        }
    }
    if !affected.is_empty() {
        return affected;
    }
    for table in database.tables() {
        let Ok(policies) = table.policies(database) else {
            affected.insert(table.table_name().to_owned());
            continue;
        };
        let mut reached = BTreeSet::new();
        let mut unreadable = false;
        for policy in policies {
            match policy.using_functions(database) {
                Ok(functions) => reached.extend(functions),
                Err(_) => unreadable = true,
            }
            match policy.check_functions(database) {
                Ok(functions) => reached.extend(functions),
                Err(_) => unreadable = true,
            }
        }
        let mut pending: Vec<_> = reached.iter().copied().collect();
        while let Some(function) = pending.pop() {
            match function.referenced_functions(database) {
                Ok(functions) => {
                    for called in functions {
                        if reached.insert(called) {
                            pending.push(called);
                        }
                    }
                }
                Err(_) => unreadable = true,
            }
        }
        if unreadable
            || reached.iter().any(|function| {
                function.name() == subject || function.target_name().to_string() == subject
            })
        {
            affected.insert(table.table_name().to_owned());
        }
    }
    if affected.is_empty() {
        affected.extend(changed.iter().cloned());
    }
    affected
}

fn refuse_readers(refused: &mut BTreeSet<String>, policies: &[PolicyReads]) {
    loop {
        let before = refused.len();
        for policy in policies {
            if (!policy.unresolved.is_empty() && !refused.is_empty())
                || policy
                    .reads
                    .iter()
                    .any(|table| refused.contains(table.name()))
            {
                refused.insert(policy.table.name().to_owned());
            }
        }
        if refused.len() == before {
            break;
        }
    }
}

impl SetupError {
    /// The service whose recovery permits a held follow to retry.
    pub(crate) fn pause_cause(&self) -> Option<connetto_core::messages::PauseCause> {
        use connetto_core::messages::PauseCause;
        match self {
            Self::Database(_) => Some(PauseCause::DatabaseUnreachable),
            Self::AuthorizationUnavailable(_) | Self::Endpoint(_) => {
                Some(PauseCause::AuthServiceUnreachable)
            }
            Self::Authorization(error)
                if crate::session::FailedAuthorization::clears_by_itself(error) =>
            {
                Some(PauseCause::AuthServiceUnreachable)
            }
            _ => None,
        }
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter"
)]
pub(super) fn query_error(error: diesel::result::Error) -> SetupError {
    if crate::reexec::diesel_failure(&error) == crate::reexec::ReadFailure::Transient {
        SetupError::Database(error.to_string())
    } else {
        SetupError::Store(error.to_string())
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter"
)]
pub(super) fn model_status(status: openfga_client::tonic::Status) -> SetupError {
    use openfga_client::tonic::Code;
    if matches!(
        status.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Cancelled
            | Code::ResourceExhausted
            | Code::Aborted
    ) {
        SetupError::AuthorizationUnavailable(status.to_string())
    } else {
        SetupError::Model(status.to_string())
    }
}

pub(super) fn reconcile_error(
    error: subql::visibility::openfga::ReconcileError<SetupError>,
) -> SetupError {
    use subql::visibility::openfga::ReconcileError;
    match error {
        ReconcileError::Replay(error) => error,
        ReconcileError::Store(error) => SetupError::Authorization(error),
        other => SetupError::Store(other.to_string()),
    }
}

fn fingerprint<Share: serde::Serialize>(share: &Share) -> Result<String, serde_json::Error> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(share)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;

    fn catalog(policies: &str) -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(&format!(
            "CREATE ROLE reader; CREATE TABLE first_table(id INT PRIMARY KEY, owner TEXT); \
             CREATE TABLE second_table(id INT PRIMARY KEY, owner TEXT); \
             GRANT SELECT ON first_table, second_table TO reader; \
             ALTER TABLE first_table ENABLE ROW LEVEL SECURITY; \
             ALTER TABLE second_table ENABLE ROW LEVEL SECURITY; {policies}"
        ))
        .expect("valid catalog")
    }

    #[test]
    fn policy_subject_reaches_every_same_named_policy() {
        let database = catalog(
            "CREATE POLICY shared ON first_table USING (true); \
             CREATE POLICY shared ON second_table USING (true);",
        );
        assert_eq!(
            subject_tables(&database, "shared", &["fallback".to_owned()]),
            BTreeSet::from(["first_table".to_owned(), "second_table".to_owned()])
        );
    }

    #[test]
    fn unknown_subject_refuses_changed_tables() {
        assert_eq!(
            subject_tables(&catalog(""), "unknown", &["first_table".to_owned()]),
            BTreeSet::from(["first_table".to_owned()])
        );
    }

    #[test]
    fn table_subject_does_not_refuse_siblings() {
        assert_eq!(
            subject_tables(&catalog(""), "second_table", &["first_table".to_owned()]),
            BTreeSet::from(["second_table".to_owned()])
        );
    }

    #[test]
    fn function_subject_follows_nested_calls() {
        let database = catalog(
            "CREATE FUNCTION leaf(value TEXT) RETURNS BOOLEAN LANGUAGE SQL AS $$ SELECT value = 'allowed' $$; \
             CREATE FUNCTION parent(value TEXT) RETURNS BOOLEAN LANGUAGE SQL AS $$ SELECT leaf(value) $$; \
             CREATE POLICY guarded ON first_table USING (parent(owner)); \
             CREATE POLICY unrelated ON second_table USING (true);",
        );
        assert_eq!(
            subject_tables(&database, "leaf", &["second_table".to_owned()]),
            BTreeSet::from(["first_table".to_owned()])
        );
    }

    fn policy(table: &str, reads: &[&str]) -> PolicyReads {
        PolicyReads {
            name: format!("{table}_policy"),
            table: rls2fga::types::TableId::from_stored(None, table.to_owned()),
            command: rls2fga::types::PolicyCommand::Select,
            reads: reads
                .iter()
                .map(|table| rls2fga::types::TableId::from_stored(None, (*table).to_owned()))
                .collect(),
            unresolved: Vec::new(),
        }
    }

    #[test]
    fn refusal_reaches_transitive_readers() {
        let policies = vec![policy("second", &["first"]), policy("third", &["second"])];
        let mut refused = BTreeSet::from(["first".to_owned()]);
        refuse_readers(&mut refused, &policies);
        assert_eq!(
            refused,
            BTreeSet::from(["first".to_owned(), "second".to_owned(), "third".to_owned()])
        );
    }

    #[test]
    fn unresolved_reader_depends_on_every_refusal() {
        let mut reader = policy("second", &[]);
        reader.unresolved.push("dynamic read".to_owned());
        let mut refused = BTreeSet::from(["first".to_owned()]);
        refuse_readers(&mut refused, &[reader]);
        assert!(refused.contains("second"));
    }

    #[test]
    fn fingerprint_uses_serialized_content() {
        let first = fingerprint(&vec!["relation", "row", "query"]).expect("serializable share");
        assert_eq!(
            first,
            fingerprint(&vec!["relation", "row", "query"]).expect("serializable share")
        );
        assert_ne!(
            first,
            fingerprint(&vec!["relation", "row", "other query"]).expect("serializable share")
        );
        assert_eq!(first.len(), 64);
    }

    const SCHEMA: &str = "CREATE TABLE documents(id INTEGER PRIMARY KEY, owner TEXT); \
        ALTER TABLE documents ENABLE ROW LEVEL SECURITY; \
        CREATE TABLE sibling(id INTEGER PRIMARY KEY, owner TEXT); \
        ALTER TABLE sibling ENABLE ROW LEVEL SECURITY;";
    const POLICY: &str = "CREATE POLICY documents_policy ON documents USING \
        (owner = current_setting('app.user_id', true)); \
        CREATE POLICY sibling_policy ON sibling USING \
        (owner = current_setting('app.user_id', true));";

    fn documents<'document>(
        schema: &'document str,
        policies: &'document str,
    ) -> connetto_translate::Documents<'document> {
        connetto_translate::Documents::new(
            schema,
            policies,
            connetto_test_harness::FIXTURE_ROLES,
            connetto_test_harness::FIXTURE_READER,
        )
    }

    fn startup() -> Translated {
        Translated::of::<String, crate::defaults::ConnettoDefaults>(
            documents(SCHEMA, POLICY),
            false,
        )
        .expect("supported document rules")
    }

    fn live_database(schema: &str, policies: &str) -> ParserDB {
        let source = crate::schema::catalog::<crate::defaults::ConnettoDefaults>(
            documents(schema, policies),
            false,
        )
        .expect("schema catalog");
        ParserDB::parse::<PostgreSqlDialect>(&source).expect("live catalog")
    }

    #[test]
    fn live_rules_keep_served_table_numbering_and_outputs() {
        let startup = startup();
        let changed_policy = POLICY.replace(
            "CREATE POLICY documents_policy ON documents USING (owner = current_setting('app.user_id', true))",
            "CREATE POLICY documents_policy ON documents USING (true)",
        );
        let live = live_database(SCHEMA, &changed_policy);
        let followed = startup
            .follow_database(&live, &["documents".to_owned()])
            .expect("live translation");
        assert_eq!(followed.refused, Vec::<String>::new());
        assert_eq!(
            startup
                .shapes
                .catalog()
                .tables()
                .map(TableLike::table_name)
                .collect::<Vec<_>>(),
            followed
                .translated
                .shapes
                .catalog()
                .tables()
                .map(TableLike::table_name)
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            serde_json::to_value(followed.translated.model()).expect("model"),
            serde_json::to_value(followed.translated.outputs.json_model()).expect("model"),
        );
        assert_ne!(
            serde_json::to_value(startup.model()).expect("model"),
            serde_json::to_value(followed.translated.model()).expect("model"),
        );
    }

    #[test]
    fn live_new_column_refuses_only_its_table() {
        let startup = startup();
        let schema = format!(
            "{SCHEMA} ALTER TABLE documents ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT true;"
        );
        let policies = POLICY.replace(
            "CREATE POLICY documents_policy ON documents USING (owner = current_setting('app.user_id', true))",
            "CREATE POLICY documents_policy ON documents USING (enabled)",
        );
        let followed = startup
            .follow_database(
                &live_database(&schema, &policies),
                &["documents".to_owned()],
            )
            .expect("live translation");
        assert_eq!(followed.refused, ["documents"]);
        assert!(followed.translated.tuples.iter().all(|query| {
            query.description.as_ref().is_none_or(|description| {
                description
                    .tables
                    .iter()
                    .all(|table| table.name() != "documents")
            })
        }));
    }

    #[test]
    fn divergent_policy_refuses_only_its_table() {
        let startup = startup();
        let policies = POLICY.replace(
            "CREATE POLICY documents_policy ON documents USING (owner = current_setting('app.user_id', true))",
            "CREATE FUNCTION mystery_function(who TEXT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$ BEGIN RETURN who IS NOT NULL; END $$; CREATE POLICY documents_policy ON documents USING (mystery_function(owner))",
        );
        let followed = startup
            .follow_database(&live_database(SCHEMA, &policies), &["documents".to_owned()])
            .expect("partial live translation");
        assert_eq!(followed.refused, ["documents"]);
        assert_ne!(
            followed.translated.policy_reads(),
            &[] as &[rls2fga::types::PolicyReads]
        );
    }

    #[test]
    fn shares_cover_served_tables_without_owning_outputs() {
        let startup = startup();
        let shares = startup.translation_shares();
        assert_eq!(shares.len(), startup.shapes.catalog().tables().count());
        assert_ne!(
            shares["documents"].naming,
            Vec::<&rls2fga::types::RowNaming>::new()
        );
        assert_ne!(
            shares["documents"].relations,
            Vec::<&rls2fga::types::RelationShapes>::new()
        );
        assert!(!shares["documents"].queries.is_empty());
    }

    #[test]
    fn typed_failures_choose_the_matching_pause() {
        use connetto_core::messages::PauseCause;
        assert_eq!(
            SetupError::Database("offline".to_owned()).pause_cause(),
            Some(PauseCause::DatabaseUnreachable)
        );
        assert_eq!(
            model_status(openfga_client::tonic::Status::unavailable("offline")).pause_cause(),
            Some(PauseCause::AuthServiceUnreachable)
        );
        assert_eq!(
            model_status(openfga_client::tonic::Status::invalid_argument("model")).pause_cause(),
            None
        );
        assert_eq!(
            SetupError::Store("invalid tuple".to_owned()).pause_cause(),
            None
        );
    }

    #[test]
    fn document_translation_stays_strict() {
        let policies = POLICY.replace(
            "CREATE POLICY documents_policy ON documents USING (owner = current_setting('app.user_id', true))",
            "CREATE FUNCTION mystery_function(who TEXT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$ BEGIN RETURN who IS NOT NULL; END $$; CREATE POLICY documents_policy ON documents USING (mystery_function(owner))",
        );
        assert!(matches!(
            Translated::of::<String, crate::defaults::ConnettoDefaults>(
                documents(SCHEMA, &policies),
                false,
            ),
            Err(SetupError::Untranslated(_))
        ));
    }

    #[test]
    fn policy_reading_unserved_table_refuses_its_reader() {
        let startup = startup();
        let schema =
            format!("{SCHEMA} CREATE TABLE permissions(id INTEGER PRIMARY KEY, owner TEXT);");
        let policies = POLICY.replace(
            "CREATE POLICY documents_policy ON documents USING (owner = current_setting('app.user_id', true))",
            "CREATE POLICY documents_policy ON documents USING (EXISTS \
             (SELECT 1 FROM permissions WHERE permissions.id = documents.id \
             AND permissions.owner = current_setting('app.user_id', true)))",
        );
        let followed = startup
            .follow_database(
                &live_database(&schema, &policies),
                &["documents".to_owned()],
            )
            .expect("partial live translation");
        assert_eq!(followed.refused, ["documents"]);
        assert!(followed.translated.policy_reads().iter().any(|policy| {
            policy.table.name() == "documents"
                && policy
                    .reads
                    .iter()
                    .any(|table| table.name() == "permissions")
        }));
    }

    #[test]
    fn retained_outputs_are_shared_with_upkeep() {
        let translated = startup();
        assert!(Arc::ptr_eq(&translated.outputs, &translated.outputs_arc()));
        assert!(Arc::ptr_eq(&translated.reach, &translated.reach_arc()));
    }

    #[test]
    fn replay_failure_preserves_database_source() {
        use connetto_core::messages::PauseCause;
        use subql::visibility::openfga::ReconcileError;
        let error = reconcile_error(ReconcileError::Replay(SetupError::Database(
            "offline".to_owned(),
        )));
        assert_eq!(error.pause_cause(), Some(PauseCause::DatabaseUnreachable));
        let error = reconcile_error(ReconcileError::Replay(SetupError::Store(
            "invalid tuple".to_owned(),
        )));
        assert_eq!(error.pause_cause(), None);
    }

    #[test]
    fn missing_parameters_refuse_only_request_gated_tables() {
        let database = catalog(
            "CREATE POLICY gated ON first_table USING \
             (owner = ANY(string_to_array(current_setting('app.subjects', true), ','))); \
             CREATE POLICY plain ON second_table USING (true);",
        );
        let deployment = connetto_translate::deployment::<String>(
            "reader",
            connetto_core::auth::UnsetSettings::ALWAYS_BOUND,
        )
        .expect("deployment");
        let translation = rls2fga::translator::TranslatorBuilder::new(deployment)
            .with_min_confidence(rls2fga::types::ConfidenceLevel::B)
            .build()
            .translate(&database)
            .expect("translation");
        let shapes = Shapes::new::<Postgres>(database.clone(), translation.relations(), &[])
            .with_row_naming(translation.row_naming());
        assert!(shapes.has_request_gated_recipe());
        assert_eq!(shapes.required_parameters(), &[]);
        let mut refused = BTreeSet::new();
        let blocked = index_refusals(
            &shapes,
            &translation,
            &["first_table".to_owned()],
            &mut refused,
        );
        assert_eq!(refused, BTreeSet::from(["first_table".to_owned()]));
        let relations: Vec<_> = translation
            .relations()
            .iter()
            .filter(|relation| !blocked.contains(relation))
            .cloned()
            .collect();
        let rebuilt = Shapes::new::<Postgres>(database, &relations, &[])
            .with_row_naming(translation.row_naming());
        assert!(!rebuilt.has_request_gated_recipe());
        assert!(relations.iter().any(|relation| {
            translation.row_naming().iter().any(|naming| {
                naming.table.name() == "second_table" && naming.type_name == relation.type_name
            })
        }));
    }

    #[test]
    fn reported_parameters_keep_request_gated_tables_served() {
        let translated = startup();
        let mut refused = BTreeSet::new();
        let blocked = index_refusals(
            &translated.shapes,
            translated.outputs.translation(),
            &["documents".to_owned()],
            &mut refused,
        );
        assert_eq!(blocked, Vec::<&rls2fga::types::RelationShapes>::new());
        assert!(refused.is_empty());
    }

    #[test]
    fn only_transient_query_failures_pause_following() {
        use connetto_core::messages::PauseCause;
        use diesel::result::DatabaseErrorKind;
        let unavailable =
            crate::reexec::coded_error(DatabaseErrorKind::Unknown, "08006", "disconnected");
        let undefined_column =
            crate::reexec::coded_error(DatabaseErrorKind::Unknown, "42703", "missing column");
        assert_eq!(
            query_error(unavailable).pause_cause(),
            Some(PauseCause::DatabaseUnreachable)
        );
        assert_eq!(query_error(undefined_column).pause_cause(), None);
        assert_eq!(
            query_error(diesel::result::Error::NotFound).pause_cause(),
            None
        );
    }
}
