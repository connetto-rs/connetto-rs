//! A change source that yields one event and then ends cleanly.

use std::io;

use subql::{CdcSource, PgChangeEvent, PgCommit, PgCommitPosition, SourceItem};

/// Yields its event once, so what the ingest loop does with that event is the
/// whole of what a run observes.
pub struct OneEvent(pub Option<PgChangeEvent>);

impl CdcSource for OneEvent {
    type Commit = PgCommit;
    type Event = PgChangeEvent;
    type Error = io::Error;

    fn next_item(
        &mut self,
    ) -> impl Future<Output = Result<Option<SourceItem<PgChangeEvent, PgCommit>>, io::Error>> + Send
    {
        let next = self.0.take();
        async move { Ok(next.map(SourceItem::Event)) }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn ack(&mut self, _upto: PgCommitPosition) -> Result<(), io::Error> {
        Ok(())
    }
}
