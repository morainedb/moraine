//! Moving a catalog's write-ahead log to another object store.
//!
//! Two opens, in this order, so no crash between them can lose a commit:
//! the first takes the log store the catalog records and writes its memtable
//! out as a sorted-string table, which leaves nothing in that log for a
//! replay to miss; the second takes the new store and records it there,
//! the only write that reaches it.

use slatedb::{
    Db, IsolationLevel,
    config::{FlushOptions, FlushType},
};
use tracing::info;

use crate::{
    error::{Error, Result},
    store::{
        StagedBytes,
        handle::ReadHandle,
        key::{EntityKey, Key},
        open::StoreBuilder,
        proto, read, value,
    },
    transaction::commit::{self, CommitDurability, commit_durable},
};

/// Where a catalog's write-ahead log was and where it is now. Equal
/// endpoints mean the log was already there and nothing moved; `None` is
/// the catalog store itself.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalStoreMove {
    /// The store the catalog recorded for its log.
    pub from: Option<String>,
    /// The store it records now.
    pub to: Option<String>,
}

impl WalStoreMove {
    /// Whether the log changed stores.
    #[must_use]
    pub fn moved(&self) -> bool {
        self.from != self.to
    }
}

/// Moves the log from the store `current` is opened against to the one
/// `next` is. Both opens take the writer, so nothing may hold the catalog
/// while this runs.
pub(crate) async fn run(
    current: StoreBuilder<'_>,
    next: StoreBuilder<'_>,
    current_name: Option<&str>,
    next_name: Option<&str>,
) -> Result<WalStoreMove> {
    let (db, _) = current.open_writer().await?;
    let recorded = closing(&db, drain_recorded_log(&db, current_name)).await?;
    if recorded.as_deref() == next_name {
        return Ok(WalStoreMove {
            from: recorded.clone(),
            to: recorded,
        });
    }

    let (db, _) = next.open_writer().await?;
    closing(&db, record_log_store(&db, next_name)).await?;

    info!(from = recorded, to = next_name, "moved the write-ahead log");
    Ok(WalStoreMove {
        from: recorded,
        to: next_name.map(str::to_owned),
    })
}

/// Awaits `work`, then closes `db` whichever way it went; a close that
/// fails on its own is reported, since an unclosed writer leaves the
/// memtable unwritten.
async fn closing<T>(db: &Db, work: impl Future<Output = Result<T>>) -> Result<T> {
    let outcome = work.await;
    let closed = db.close().await.map_err(Error::from);
    outcome.and_then(|value| closed.map(|()| value))
}

/// Validates that this catalog's log is the one `db` was opened against,
/// reports the store it records, and writes the memtable out so that log
/// holds nothing a later open has to replay.
async fn drain_recorded_log(db: &Db, current_name: Option<&str>) -> Result<Option<String>> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    let read = async {
        let format = commit::validate_format(ReadHandle::Tx(&tx)).await?;
        if format.is_none() {
            return Err(Error::Corruption(
                "store is not an initialized moraine catalog; there is no write-ahead log to \
                 move"
                    .to_string(),
            ));
        }
        commit::validate_wal_store(ReadHandle::Tx(&tx), current_name).await?;
        commit::recorded_wal_store(ReadHandle::Tx(&tx)).await
    }
    .await;
    tx.rollback();
    let recorded = read?;

    flush_memtable(db).await?;
    Ok(recorded)
}

/// Records `name` as the catalog's log store, in the log store `db` was
/// opened against, and writes it out as a sorted-string table.
async fn record_log_store(db: &Db, name: Option<&str>) -> Result<()> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    // The global record carries every catalog-wide option, so it is read,
    // amended, and written whole.
    let staged = async {
        let mut options = read::read_global_options(ReadHandle::Tx(&tx))
            .await?
            .unwrap_or_default()
            .options;
        match name {
            Some(name) => options.insert("wal_path".to_string(), name.to_string()),
            None => options.remove("wal_path"),
        };
        let key = Key::current(EntityKey::Option {
            scope_kind: 0,
            scope_id: 0,
        })
        .encode();
        let value = value::encode_value(&proto::OptionScopeValue { options });
        let mut staged = StagedBytes::default();
        staged.add(key.len(), value.len());
        tx.put(key, value).map_err(Error::from)?;
        Ok::<_, Error>(staged)
    }
    .await;
    let staged = match staged {
        Ok(staged) => staged,
        Err(error) => {
            tx.rollback();
            return Err(error);
        }
    };

    commit_durable(
        tx,
        "write-ahead log move",
        staged,
        &CommitDurability::OnFlushInterval,
    )
    .await?;
    flush_memtable(db).await
}

/// Writes the memtable out as a sorted-string table, which is what carries
/// the log's contents into the catalog store and moves the store's replay
/// point past them.
async fn flush_memtable(db: &Db) -> Result<()> {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .map_err(Error::from)
}
