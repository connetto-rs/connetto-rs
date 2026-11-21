//! The demo's `users` table, which [`DemoHooks`] writes inside each sign-in's
//! own transaction.

use connetto_server::{
    AuthHooks, ConnettoSchema, HookError, HookFuture, VerifiedClaims, deterministic_uuid,
};
use diesel::ExpressionMethods as _;
use diesel_async::{AsyncPgConnection, RunQueryDsl as _};

diesel::table! {
    /// One row per account that ever signed in, `examples/deployment/schema.sql`.
    users (id) {
        /// The account's id, the v5 UUID over the provider's issuer and the
        /// login subject.
        id -> Text,
        /// The subject the account signed in as.
        login_name -> Text,
    }
}

/// The demo server's auth hooks. Each sign-in maps to the v5 UUID the default
/// hooks answer, so accounts keep their ids, and registers the account in
/// `users` with its subject as the login name. The row commits with the
/// session or not at all.
#[derive(Debug, Clone, Copy, Default)]
pub struct DemoHooks;

impl<D: ConnettoSchema<Id = String>> AuthHooks<D> for DemoHooks {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        Box::pin(async move {
            let user_id = deterministic_uuid(&claims.issuer, &claims.subject).to_string();
            diesel::insert_into(users::table)
                .values((
                    users::id.eq(&user_id),
                    users::login_name.eq(&claims.subject),
                ))
                .on_conflict(users::id)
                .do_nothing()
                .execute(transaction)
                .await
                .map_err(|error| HookError::Backend(format!("registering the user: {error}")))?;
            Ok(user_id)
        })
    }
}
