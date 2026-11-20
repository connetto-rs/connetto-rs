//! The compile-time dispatched `live()` verb.
//!
//! [`Watchable`] gives every typed diesel query a postfix
//! `query.live(&client)` that returns the right live handle for the query's
//! shape, chosen by the type system: a row projection produces a
//! [`LiveQuery`], a scalar aggregate produces a [`LiveValue`] and a grouped
//! aggregate a [`LiveGroups`]. A misrouted query does not compile.
//!
//! The dispatch projects diesel's own aggregation discriminator: a built
//! [`SelectStatement`]'s select clause exposes its expression through
//! [`SelectClauseExpression`], and that expression's
//! [`ValidGrouping`]`<()>::IsAggregate` names one of the [`is_aggregate`]
//! markers. [`SelectionMarker`] captures both, and [`WatchDispatch`] is keyed
//! on the marker as a type parameter so the row, scalar and grouped impls
//! never overlap.
//!
//! Every computed value rests in the replica's storage form and loads through
//! diesel with the query's own SQL types, so any type that loads from the
//! query with `get_result` or `load` is a valid value, key or row type,
//! application types and composite keys included. Annotate it on the binding,
//! as with diesel's `load`.
//!
//! Boxed queries (`.into_boxed()`) erase their select clause into a trait
//! object, so no type-level marker survives boxing. Dynamic queries use the
//! explicit, runtime-guarded [`ConnettoClient::watch`],
//! [`ConnettoClient::watch_value`] and [`ConnettoClient::watch_groups`]
//! instead.

use core::future::Future;

use connetto_core::traits::{MaybeSend, Transport};
use diesel::expression::{Expression, ValidGrouping, is_aggregate};
use diesel::query_builder::{
    AsQuery, GroupByClause, NoGroupByClause, Query, QueryFragment, SelectClauseExpression,
    SelectStatement,
};
use diesel::query_dsl::methods::LoadQuery;
use diesel::sqlite::Sqlite;

use crate::ClientError;
use crate::live::{ConnettoClient, LiveGroups, LiveHandle, LiveQuery, LiveValue, Loadable};

/// Compile-time dispatch marker for a grouped query, carrying its `GROUP BY`
/// expression's SQL type.
pub struct Grouped<G>(core::marker::PhantomData<G>);
/// The selection expression and aggregation marker of a built select
/// statement, projected purely at the type level.
pub trait SelectionMarker {
    /// The select clause's expression type.
    type Selection;
    /// One of the [`is_aggregate`] marker types for that expression.
    type Marker;
}

impl<F, S, D, W, O, LOf, H, LC> SelectionMarker
    for SelectStatement<F, S, D, W, O, LOf, NoGroupByClause, H, LC>
where
    S: SelectClauseExpression<F>,
    S::Selection: ValidGrouping<()>,
{
    type Selection = S::Selection;
    type Marker = <S::Selection as ValidGrouping<()>>::IsAggregate;
}

impl<F, S, D, W, O, LOf, G, H, LC> SelectionMarker
    for SelectStatement<F, S, D, W, O, LOf, GroupByClause<G>, H, LC>
where
    S: SelectClauseExpression<F>,
    G: Expression,
    S::Selection: ValidGrouping<G>,
{
    type Selection = S::Selection;
    type Marker = Grouped<G::SqlType>;
}

/// The SQL type of a grouped selection split into its key columns and its
/// last column, the aggregate a keyed map holds as its value.
pub trait SplitLast {
    /// The SQL type of every column but the last: the bare type for one, a
    /// tuple for several.
    type Init;
    /// The SQL type of the last column.
    type Last;
}

/// Implement [`SplitLast`] for one tuple arity.
macro_rules! split_last {
    ($first:ident ; $last:ident) => {
        impl<$first, $last> SplitLast for ($first, $last) {
            type Init = $first;
            type Last = $last;
        }
    };
    ($($init:ident),+ ; $last:ident) => {
        impl<$($init,)+ $last> SplitLast for ($($init,)+ $last) {
            type Init = ($($init,)+);
            type Last = $last;
        }
    };
}

split_last!(A; V);
split_last!(A, B; V);
split_last!(A, B, C; V);
split_last!(A, B, C, D; V);
split_last!(A, B, C, D, E; V);
split_last!(A, B, C, D, E, F; V);
split_last!(A, B, C, D, E, F, G; V);
split_last!(A, B, C, D, E, F, G, H; V);

/// Dispatch machinery behind [`Watchable`], keyed on the aggregation marker
/// `M` as a type parameter so the row, scalar and grouped impls stay
/// coherent. `R` is the handle's element type: the row type for a row query,
/// the value type for a scalar aggregate, and the key and value pair for a
/// grouped one.
pub trait WatchDispatch<T: Transport, M, R>: Sized {
    /// The live handle this dispatch produces.
    type Handle: LiveHandle;

    /// Subscribe through `client` and return the live handle.
    fn dispatch<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<Self::Handle, ClientError>> + MaybeSend + 'a
    where
        Self: 'a;
}

impl<T, Q, R> WatchDispatch<T, is_aggregate::No, R> for Q
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    Q: QueryFragment<Sqlite> + Clone + Send + 'static,
    Q: for<'query> LoadQuery<'query, diesel::SqliteConnection, R>,
    R: Clone + PartialEq + Send + Sync + 'static,
{
    type Handle = LiveQuery<R>;

    fn dispatch<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<Self::Handle, ClientError>> + MaybeSend + 'a
    where
        Self: 'a,
    {
        client.watch(self)
    }
}

impl<T, Q, V> WatchDispatch<T, is_aggregate::Yes, V> for Q
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    Q: AsQuery + Query + QueryFragment<Sqlite> + Send + 'static,
    Q::Query: SelectionMarker,
    V: Loadable<<Q as Query>::SqlType> + Clone + PartialEq + Send + Sync,
{
    type Handle = LiveValue<V>;

    fn dispatch<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<Self::Handle, ClientError>> + MaybeSend + 'a
    where
        Self: 'a,
    {
        client.watch_value_typed(self)
    }
}

impl<T, Q, G, K, V> WatchDispatch<T, Grouped<G>, (K, V)> for Q
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    Q: AsQuery + Query + QueryFragment<Sqlite> + Send + 'static,
    Q::Query: SelectionMarker,
    <Q as Query>::SqlType: SplitLast,
    K: Loadable<<<Q as Query>::SqlType as SplitLast>::Init>
        + Eq
        + core::hash::Hash
        + Clone
        + Send
        + Sync,
    V: Loadable<<<Q as Query>::SqlType as SplitLast>::Last> + Clone + PartialEq + Send + Sync,
{
    type Handle = LiveGroups<K, V>;

    fn dispatch<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<Self::Handle, ClientError>> + MaybeSend + 'a
    where
        Self: 'a,
    {
        client.watch_groups_typed(self)
    }
}

/// A typed diesel query that can go live with `query.live(&client)`.
///
/// Implemented for every non-boxed select statement. The handle type is
/// chosen at compile time from the query's aggregation marker: row
/// projections produce a [`LiveQuery`], scalar aggregates a [`LiveValue`]
/// and grouped aggregates a [`LiveGroups`]. Annotate the row, value or key
/// and value types on the binding, as with diesel's `load`.
pub trait Watchable<T: Transport, R>: Sized {
    /// The live handle this query produces.
    type Handle: LiveHandle;

    /// Subscribe and return the live handle. See
    /// [`ConnettoClient::watch`], [`ConnettoClient::watch_value`] and
    /// [`ConnettoClient::watch_groups`] for the underlying contracts of the
    /// handle kinds.
    fn live<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<Self::Handle, ClientError>> + MaybeSend + 'a
    where
        Self: 'a;
}

impl<T, Q, R> Watchable<T, R> for Q
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    Q: AsQuery,
    Q::Query: SelectionMarker,
    Q: WatchDispatch<T, <Q::Query as SelectionMarker>::Marker, R>,
{
    type Handle = <Q as WatchDispatch<T, <Q::Query as SelectionMarker>::Marker, R>>::Handle;

    fn live<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<Self::Handle, ClientError>> + MaybeSend + 'a
    where
        Self: 'a,
    {
        self.dispatch(client)
    }
}
