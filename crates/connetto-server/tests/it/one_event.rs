use subql::{CdcSource, PgChangeEvent, PgCommit, PgCommitPosition, SourceItem};

/// A source that yields one event and then ends cleanly, so what the ingest loop
/// does with that event is the whole of what a run observes.
pub(crate) struct OneEvent(pub(crate) Option<PgChangeEvent>);

impl CdcSource for OneEvent {
    type Commit = PgCommit;
    type Event = PgChangeEvent;
    type Error = std::io::Error;

    fn next_item(
        &mut self,
    ) -> impl Future<Output = Result<Option<SourceItem<PgChangeEvent, PgCommit>>, std::io::Error>> + Send
    {
        let next = self.0.take();
        async move { Ok(next.map(SourceItem::Event)) }
    }

    // reason: the trait wants a `Send` future and clippy's other arm wants
    // `async fn`, so one of the two fires whichever form this takes.
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn ack(&mut self, _upto: PgCommitPosition) -> Result<(), std::io::Error> {
        Ok(())
    }
}
