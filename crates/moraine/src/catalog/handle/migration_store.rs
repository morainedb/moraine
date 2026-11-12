//! Writer acquisition that permits resuming a recorded migration.

use super::{
    Arc, CatalogOptions, Db, Error, LogCommitter, ObjectStore, ReadHandle, Result, catalog_hooks,
    census, commit, commit_log_provider, wal_object_store, writer_store_builder,
};

async fn open_raw(
    object_store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
) -> Result<(Db, Option<Arc<dyn crate::WriterSession>>)> {
    let builder = writer_store_builder(
        &object_store,
        options,
        wal_object_store(options),
        Vec::new(),
    );
    let session = if let Some(provider) = commit_log_provider(options) {
        let session = provider.writer_session(builder.log_store(), &options.path);
        if session.adopted().await? {
            Some(session)
        } else {
            None
        }
    } else {
        None
    };
    let (db, _) = builder
        .flush_interval(options.flush_interval)
        .writer_session(session.clone())
        .open_writer()
        .await?;
    let validated = async {
        commit::verify_wal_store(&db, options.wal_store.as_ref().map(crate::WalStore::name))
            .await?;
        let format = crate::store::read::read_format(ReadHandle::Writer(&db))
            .await?
            .ok_or_else(|| {
                Error::Corruption("store is not an initialized moraine catalog".into())
            })?;
        if format.format_version > commit::MAX_FORMAT_VERSION {
            return Err(Error::Migration(
                "store format is newer than this binary understands".into(),
            ));
        }
        if commit::uses_commit_log(&format) != session.is_some() {
            return Err(Error::Configuration(
                "migration requires the catalog's recorded commit-log provider".into(),
            ));
        }
        Ok(())
    }
    .await;
    if let Err(error) = validated {
        let _ = db.close().await;
        return Err(error);
    }
    Ok((db, session))
}

pub(super) async fn open(
    object_store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
) -> Result<(Db, Option<LogCommitter>)> {
    let (db, session) = open_raw(object_store.clone(), options).await?;
    let Some(session) = session else {
        return Ok((db, None));
    };
    let provider = commit_log_provider(options)
        .ok_or_else(|| Error::Configuration("migration lost its commit-log provider".into()))?;
    let batch_origin = commit::batch_origin(ReadHandle::Writer(&db)).await?;
    let mut hooks = catalog_hooks(&object_store, options);
    hooks.take = Arc::new(Migrator {
        object_store,
        options: options.clone(),
    });
    let store = provider.holding(db.clone(), session, hooks);
    Ok((
        db,
        Some(LogCommitter {
            store,
            batch_origin,
        }),
    ))
}

struct Migrator {
    object_store: Arc<dyn ObjectStore>,
    options: CatalogOptions,
}

#[async_trait::async_trait]
impl crate::TakeStore for Migrator {
    async fn take(&self) -> Result<Option<(Db, Arc<dyn crate::WriterSession>)>> {
        let ceiling = slatedb::config::Settings::default().l0_max_ssts;
        let files = census::l0_files(&self.options.path, self.object_store.clone()).await?;
        if usize::try_from(files)
            .unwrap_or(usize::MAX)
            .saturating_add(1)
            >= ceiling
        {
            return Ok(None);
        }
        let (db, session) = open_raw(self.object_store.clone(), &self.options).await?;
        let session = session
            .ok_or_else(|| Error::Configuration("migration requires a commit log".into()))?;
        Ok(Some((db, session)))
    }
}
