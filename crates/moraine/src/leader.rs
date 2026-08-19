//! The leader role: an advisory, self-appointed, log-discovered group-commit
//! funnel a long-lived folder opens.
//!
//! A process holding the fenced writer in a long-lived posture binds a port,
//! mints or reads the store-held forwarding token, and announces itself through
//! the commit log; forwarded sessions then read their premise through it, so
//! their commits chain and coalesce into shared slots. Nothing in the direct or
//! folder commit path depends on this: with the `leader` feature off the whole
//! module is absent, and the fleet is purely direct.
//!
//! Appointment rides the folder posture — an ephemeral fold sprint never
//! announces. Announcement is one advert riding a slot; a clean shutdown writes
//! an endpoint-absent advert, an explicit withdrawal a peer can tell from a
//! slot carrying no advert at all. There are no heartbeats.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use moraine_wal::LeaderAdvert;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    catalog::{Catalog, SlotStore},
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{Key, SysKey},
        proto, read, value,
    },
    transaction::{commit, folder},
};

/// How often a serving leader re-reads the advert to notice a fresher rival
/// announcement and stand down. Advisory and coarse — the tail scan is one
/// LIST, and a duel that resolves a poll late only wastes throughput, never a
/// commit.
const SUPERSESSION_POLL: Duration = Duration::from_secs(1);

/// A bound on how long a stand-down waits for admitted sessions to settle
/// before withdrawing. A durable slot PUT precedes every acknowledgement, so an
/// idle connection that outlasts this loses no committed work when the drain
/// gives up.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

mod funnel;
mod session;

#[cfg(test)]
mod bench;
#[cfg(test)]
mod forward_tests;
#[cfg(test)]
mod standdown_tests;
#[cfg(test)]
mod tests;

use funnel::CommitFunnel;
use session::SessionContext;

/// Live counters a serving leader publishes for its host to report: the
/// forwarded sessions open right now and the commits landed through the funnel
/// since bind. Shared through an `Arc`; read at any time, lock-free.
#[derive(Debug, Default)]
pub struct LeaderStats {
    active_sessions: AtomicI64,
    forwarded_commits: AtomicU64,
}

impl LeaderStats {
    /// Forwarded sessions open right now.
    #[must_use]
    pub fn active_sessions(&self) -> u64 {
        u64::try_from(self.active_sessions.load(Ordering::Relaxed).max(0)).unwrap_or(0)
    }

    /// Commits landed through the funnel since this leader bound.
    #[must_use]
    pub fn forwarded_commits(&self) -> u64 {
        self.forwarded_commits.load(Ordering::Relaxed)
    }

    fn session_opened(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    fn session_closed(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_forwarded(&self, commits: u64) {
        self.forwarded_commits.fetch_add(commits, Ordering::Relaxed);
    }
}

/// Increments the active-session gauge for the life of a session, decrementing
/// it however the session ends.
pub(crate) struct SessionGuard(Arc<LeaderStats>);

impl SessionGuard {
    pub(crate) fn open(stats: Arc<LeaderStats>) -> Self {
        stats.session_opened();
        Self(stats)
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.0.session_closed();
    }
}

/// How a leader binds and advertises itself.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LeaderConfig {
    /// The local address to bind the listener to.
    pub bind_address: String,
    /// The address announced in the log; may differ from the bind address
    /// (NAT, containers).
    pub advertise_address: String,
    /// The most concurrent forwarded sessions this leader serves.
    pub max_sessions: usize,
    /// How often the serving leader re-reads the advert to notice a fresher
    /// rival and stand down.
    pub supersession_poll: Duration,
}

impl LeaderConfig {
    /// A config binding and advertising `address`, serving up to
    /// `max_sessions`.
    #[must_use]
    pub fn new(address: impl Into<String>, max_sessions: usize) -> Self {
        let address = address.into();
        Self {
            bind_address: address.clone(),
            advertise_address: address,
            max_sessions,
            supersession_poll: SUPERSESSION_POLL,
        }
    }
}

/// A bound leader: the listener, the group-commit funnel, this instance's id,
/// and the forwarding token, ready to [`serve`](Self::serve).
pub struct Leader {
    instance: [u8; 16],
    catalog: Arc<Catalog>,
    funnel: Arc<CommitFunnel>,
    secret: [u8; commit::SECRET_LEN],
    listener: TcpListener,
    advertise_address: String,
    max_sessions: usize,
    supersession_poll: Duration,
    stats: Arc<LeaderStats>,
}

impl std::fmt::Debug for Leader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leader")
            .field("instance", &Uuid::from_bytes(self.instance))
            .field("advertise_address", &self.advertise_address)
            .field("max_sessions", &self.max_sessions)
            .finish_non_exhaustive()
    }
}

impl Leader {
    /// Binds the listener, mints or reads the forwarding token, and announces
    /// this instance through the log. A read-only catalog cannot lead.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog is read-only, the address cannot be
    /// bound, the token cannot be minted, or the announcement cannot be
    /// written.
    pub async fn bind(catalog: Arc<Catalog>, config: LeaderConfig) -> Result<Self> {
        let store = catalog.slot_store();
        if store.read_only {
            return Err(Error::Constraint(
                "a read-only catalog cannot lead; it has no writer to mint the token".into(),
            ));
        }

        let secret = ensure_secret(store).await?;
        let listener = TcpListener::bind(&config.bind_address)
            .await
            .map_err(|err| Error::Configuration(format!("leader could not bind: {err}")))?;

        let instance = Uuid::new_v4().into_bytes();
        let stats = Arc::new(LeaderStats::default());
        let funnel = CommitFunnel::new(
            Arc::clone(&catalog),
            store.options.commit_batch_window,
            Arc::clone(&stats),
        );
        funnel
            .write_advert(LeaderAdvert {
                instance,
                endpoint: Some(config.advertise_address.clone()),
            })
            .await?;
        info!(
            instance = %Uuid::from_bytes(instance),
            advertise = config.advertise_address,
            "leader announced"
        );

        Ok(Self {
            instance,
            catalog,
            funnel,
            secret,
            listener,
            advertise_address: config.advertise_address,
            max_sessions: config.max_sessions,
            supersession_poll: config.supersession_poll,
            stats,
        })
    }

    /// The live counters this leader publishes — forwarded sessions open now
    /// and commits landed through the funnel. Cloned out before [`serve`] takes
    /// the leader by value, so a host keeps reading them while it serves.
    ///
    /// [`serve`]: Self::serve
    #[must_use]
    pub fn stats(&self) -> Arc<LeaderStats> {
        Arc::clone(&self.stats)
    }

    /// The address the listener actually bound (a `:0` bind resolves here).
    ///
    /// # Errors
    ///
    /// Returns an error if the local address cannot be read.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|err| Error::Configuration(format!("leader local address: {err}")))
    }

    /// This instance's id, as announced in the log.
    #[must_use]
    pub fn instance(&self) -> [u8; 16] {
        self.instance
    }

    /// The forwarding token this leader authenticates sessions against.
    #[cfg(test)]
    pub(crate) fn secret(&self) -> [u8; commit::SECRET_LEN] {
        self.secret
    }

    /// Accepts and serves forwarded sessions until `shutdown` fires or a
    /// fresher rival advert supersedes this instance, then drains admitted
    /// sessions and stands down. The stand-down withdraws only while this
    /// instance is still the effective leader: a fresher rival advert
    /// already speaks for the fleet, and a withdrawal riding a later slot
    /// would shadow it. Supersession and fencing resolve the same way — a
    /// long-lived successor that takes the folder role announces, and its
    /// later-slot advert wins.
    ///
    /// # Errors
    ///
    /// Returns an error if the withdrawal cannot be written; a per-session
    /// failure is logged, never propagated.
    pub async fn serve(self, shutdown: Arc<Notify>) -> Result<()> {
        let max_sessions = self.max_sessions.max(1);
        let sessions = Arc::new(Semaphore::new(max_sessions));
        let context = Arc::new(SessionContext {
            secret: self.secret,
            funnel: Arc::clone(&self.funnel),
            catalog: Arc::clone(&self.catalog),
            stats: Arc::clone(&self.stats),
        });

        let mut monitor = tokio::time::interval(self.supersession_poll);
        // The interval's first tick is immediate; consume it so the first check
        // waits a full poll, giving a rival time to announce.
        monitor.tick().await;

        loop {
            tokio::select! {
                () = shutdown.notified() => break,
                _ = monitor.tick() => {
                    if self.superseded_by_rival().await {
                        info!(
                            instance = %Uuid::from_bytes(self.instance),
                            "leader superseded by a fresher advert; standing down"
                        );
                        break;
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(pair) => pair,
                        Err(err) => {
                            warn!(error = %err, "leader accept failed");
                            continue;
                        }
                    };
                    spawn_session(&sessions, &context, stream);
                }
            }
        }

        // Drain: stop accepting and let admitted sessions settle by reclaiming
        // every permit. A durable slot PUT precedes every acknowledgement, so an
        // idle-but-open connection that outlasts the bound loses no committed
        // work when the drain gives up.
        let permits = u32::try_from(max_sessions).unwrap_or(u32::MAX);
        let _ = tokio::time::timeout(
            DRAIN_TIMEOUT,
            Arc::clone(&sessions).acquire_many_owned(permits),
        )
        .await;

        // Withdraw only while still the effective leader; a fresher rival advert
        // already names the fleet's leader, and clobbering it would send clients
        // direct.
        if self.superseded_by_rival().await {
            info!(
                instance = %Uuid::from_bytes(self.instance),
                "leader stood down without withdrawing; a fresher advert already leads"
            );
            return Ok(());
        }

        self.funnel
            .write_advert(LeaderAdvert {
                instance: self.instance,
                endpoint: None,
            })
            .await?;
        info!(instance = %Uuid::from_bytes(self.instance), "leader withdrew");
        Ok(())
    }

    /// Whether the log's freshest advert is a live announcement from another
    /// instance — the one signal that stands this leader down. A withdrawal
    /// (endpoint-absent) or this instance's own advert does not; nor does a
    /// direct commit above the advert, which carries no advert and proves no
    /// leader dead. A read blip retains leadership rather than abandon a
    /// healthy leader on a transient error.
    async fn superseded_by_rival(&self) -> bool {
        match current_advert(self.catalog.slot_store()).await {
            Ok(Some(advert)) => advert.endpoint.is_some() && advert.instance != self.instance,
            Ok(None) => false,
            Err(err) => {
                warn!(error = %err, "leader could not read the advert; retaining leadership this poll");
                false
            }
        }
    }
}

/// Spawns one session under a permit, so at most `max_sessions` run at once. A
/// session that fails is logged; its connection drops with no trace.
fn spawn_session(sessions: &Arc<Semaphore>, context: &Arc<SessionContext>, stream: TcpStream) {
    let sessions = Arc::clone(sessions);
    let context = Arc::clone(context);
    tokio::spawn(async move {
        let Ok(permit) = sessions.acquire_owned().await else {
            return;
        };
        if let Err(err) = session::serve(stream, &context).await {
            warn!(error = %err, "forwarded session ended in error");
        }
        drop(permit);
    });
}

/// Reads the store-held token, minting it lazily the first time a leader binds
/// a store that predates it (migration does not add it). The mint rides the
/// fenced folder writer — the leader already holds the folder role — so it is a
/// head-preserving direct write, readable by anyone with bucket access.
async fn ensure_secret(store: &SlotStore) -> Result<[u8; commit::SECRET_LEN]> {
    if let Some(token) = read_token(store).await? {
        return Ok(token);
    }

    let key = Key::Sys(SysKey::Secret).encode();
    let mint = folder::with_folder(store, async move |db: &slatedb::Db| {
        let tx = db
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)?;
        // Read-then-create under the single fenced writer: a concurrent minter
        // fences this session rather than racing the same key.
        let token = if let Some(bytes) = tx.get(&key).await.map_err(Error::from)? {
            token_bytes(&value::decode_value::<proto::SecretValue>(&bytes)?)?
        } else {
            let token = commit::mint_secret();
            tx.put(
                key.clone(),
                value::encode_value(&proto::SecretValue {
                    token: token.to_vec(),
                }),
            )
            .map_err(Error::from)?;
            token
        };
        commit::commit_durably(tx).await.map_err(Error::from)?;
        Ok(token)
    })
    .await;

    if let Ok(token) = mint {
        return Ok(token);
    }
    // A competing minter fenced this session; it wrote the authoritative token,
    // which a fresh read now returns.
    read_token(store)
        .await?
        .ok_or_else(|| Error::Corruption("forwarding token missing after a contended mint".into()))
}

/// The token read through a reader opened at the current manifest, so a mint a
/// peer just committed is seen rather than lagged behind the attach reader.
async fn read_token(store: &SlotStore) -> Result<Option<[u8; commit::SECRET_LEN]>> {
    let reader =
        crate::store::open::StoreBuilder::new(&store.options.path, store.object_store.clone())
            .cache_dir(store.options.cache_dir.clone())
            .open_reader()
            .await?;
    let secret = read::read_secret(ReadHandle::Reader(&reader)).await;
    if let Err(err) = reader.close().await {
        warn!(error = %err, "could not close the reader opened for the token read");
    }
    secret?.map(|value| token_bytes(&value)).transpose()
}

/// The 32-byte token from a stored value, refusing a wrong width.
fn token_bytes(value: &proto::SecretValue) -> Result<[u8; commit::SECRET_LEN]> {
    <[u8; commit::SECRET_LEN]>::try_from(value.token.as_slice()).map_err(|_| {
        Error::Corruption(format!(
            "forwarding token is {} bytes, expected {}",
            value.token.len(),
            commit::SECRET_LEN
        ))
    })
}

/// The current leader advert: the freshest advert riding the unfolded tail, or
/// the folded [`SysKey::Leader`] when the tail carries none. A withdrawal (an
/// endpoint-absent advert) is returned as such — distinct from `None`, which
/// says the log names no leader at all. The client-side forwarding trigger and
/// the serving leader's supersession monitor both read through it.
pub(crate) async fn current_advert(store: &SlotStore) -> Result<Option<LeaderAdvert>> {
    let handle = ReadHandle::Reader(&store.reader);
    let fold = read::read_fold(handle)
        .await?
        .map_or(0, |fold| fold.folded_sequence);
    let tail = store.slots.read_tail(fold.saturating_add(1)).await?;
    let freshest = tail
        .slots
        .iter()
        .rev()
        .find_map(|(_, envelope)| envelope.leader.clone());
    if let Some(advert) = freshest {
        return Ok(Some(advert));
    }

    let folded =
        read::read_singleton::<proto::LeaderValue>(handle, Key::Sys(SysKey::Leader)).await?;
    folded
        .map(|value| {
            let instance = <[u8; 16]>::try_from(value.instance.as_slice()).map_err(|_| {
                Error::Corruption(format!(
                    "folded leader instance id is {} bytes, expected 16",
                    value.instance.len()
                ))
            })?;
            Ok(LeaderAdvert {
                instance,
                endpoint: value.endpoint,
            })
        })
        .transpose()
}
