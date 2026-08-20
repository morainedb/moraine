//! The commit-slot log: fixed-width sequence naming, the create-if-absent
//! race that arbitrates each slot, tail enumeration with hole detection,
//! and oldest-first prefix truncation.

use std::sync::Arc;

use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};

use crate::{envelope::Envelope, error::Error};

/// The path segment every slot object lives under.
const COMMITS: &str = "commits";

/// Sequence names are zero-padded to the decimal width of `u64::MAX`, so
/// lexicographic order over names is numeric order over sequences.
const SEQUENCE_WIDTH: usize = 20;

/// The lowest sequence a log holds. Reads start here: a `from` below it is
/// raised to it, since "from 0" can only mean "from the beginning".
/// Truncation deliberately reaches below it.
const FIRST_SEQUENCE: u64 = 1;

/// A commit-slot log: totally ordered, immutable slots under
/// `<root>/commits/`, each written exactly once with a create-if-absent
/// conditional put.
///
/// The conditional put is the whole arbitration mechanism: exactly one
/// committer can win each sequence, however many race it. What a slot's
/// payload means is the embedder's business; this type owns only the shape.
#[derive(Debug, Clone)]
pub struct SlotLog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

/// The outcome of racing one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRace {
    /// The put landed.
    Won,
    /// The store reported the sequence already taken.
    Lost,
}

/// The resolved outcome of racing one slot; a loss carries the winning
/// envelope to rebase onto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// This attempt's transaction is committed at this sequence.
    Won,
    /// Another envelope holds this sequence; here it is.
    Lost(Envelope),
}

/// A tail read: the contiguous run from the requested sequence, plus
/// whether the log continues past a hole.
#[derive(Debug, Clone)]
pub struct Tail {
    /// The contiguous run from the requested sequence, ascending.
    pub slots: Vec<(u64, Envelope)>,
    /// A sequence absent while higher sequences exist: a slot was destroyed
    /// out from under the protocol. Never an end of log — callers must fail
    /// loudly, never serve `slots` as if it were the head.
    ///
    /// Set relative to the requested `from`, which must therefore sit at or
    /// above the truncation horizon; below it a deleted prefix is
    /// indistinguishable from destroyed slots and reports here.
    pub gap_at: Option<u64>,
}

impl SlotLog {
    /// A log rooted at `root`, with slots under `<root>/commits/`. An empty
    /// `root` puts them at `commits/`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, root: &str) -> Self {
        let prefix: Path = root
            .split('/')
            .filter(|part| !part.is_empty())
            .chain(std::iter::once(COMMITS))
            .collect();

        Self { store, prefix }
    }

    /// The object path of one sequence.
    pub(crate) fn slot_path(&self, sequence: u64) -> Path {
        self.prefix
            .clone()
            .join(format!("{sequence:0SEQUENCE_WIDTH$}"))
    }

    /// Races `sequence` with a create-if-absent put.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the put neither landed nor reported
    /// the slot already taken; the put's outcome is then unknown, which is
    /// what [`SlotLog::commit_slot`] exists to resolve.
    pub async fn put_slot(&self, sequence: u64, envelope: &Envelope) -> Result<SlotRace, Error> {
        let bytes = envelope.encode();
        let options = PutOptions::from(PutMode::Create);
        match self
            .store
            .put_opts(&self.slot_path(sequence), bytes.into(), options)
            .await
        {
            Ok(_) => Ok(SlotRace::Won),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(SlotRace::Lost),
            Err(err) => Err(Error::transport(format!("slot {sequence}: {err}"))),
        }
    }

    /// Reads one slot; `None` when the sequence has not been won.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the get failed, or
    /// [`Error::Corruption`] if the object is not a valid envelope.
    pub async fn read_slot(&self, sequence: u64) -> Result<Option<Envelope>, Error> {
        match self.store.get(&self.slot_path(sequence)).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|err| Error::transport(format!("slot {sequence}: {err}")))?;

                Ok(Some(Envelope::decode(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::transport(format!("slot {sequence}: {err}"))),
        }
    }

    /// Deletes one slot. Slots are immutable, so deletion is idempotent: an
    /// already-absent slot is not an error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the delete failed.
    pub async fn delete_slot(&self, sequence: u64) -> Result<(), Error> {
        self.delete_if_present(sequence).await?;

        Ok(())
    }

    /// Races `sequence`, resolving every way a put's outcome can be
    /// unreadable.
    ///
    /// A landed put is `Won`. Anything else leaves one question — the slot is
    /// taken, or may be; is it taken by *this* attempt? — and the answer comes
    /// from the log rather than from the store's verdict: re-read the slot, and
    /// an envelope carrying `envelope`'s transaction ids is this attempt
    /// (`Won`); an envelope carrying none of them is `Lost`; an absent slot
    /// means the put did not land, and the transport error surfaces.
    ///
    /// A store reporting the sequence already taken is not proof this attempt's
    /// put failed to land: the retries below this call re-issue a
    /// create-if-absent whose predecessor may already have landed, and stores
    /// answer that with the same code they use for an ordinary lost race. So
    /// both cases run the identity check, and a retry of an attempt whose
    /// earlier put went unattributed is told it holds the sequence instead of
    /// being handed its own envelope as a rival's.
    ///
    /// Resolution matches on identity, so it rests on two caller invariants.
    /// Every transaction id must be unique across the log, and an id must be
    /// offered to one committer only. An `envelope` with no commits has no
    /// identity at all: the put is accepted, but nothing in the log can then
    /// settle a taken slot either way, and this call says so rather than
    /// guessing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] when the outcome could not be read back:
    /// - the put failed and the slot reads absent, so the commit did not land;
    /// - the slot was reported taken but its object is not yet readable, which
    ///   is an ordinary contended round, not damage;
    /// - the slot is taken and `envelope` carries no transaction id to tell
    ///   whose it is.
    ///
    /// A read-back that itself fails surfaces too, and that one leaves the
    /// put's outcome unknown: this attempt may already hold the sequence.
    /// Racing it again under the same transaction ids resolves it, and
    /// [`SlotLog::find_transaction`] answers it outright.
    ///
    /// Returns [`Error::Corruption`] if a slot's bytes are not a valid
    /// envelope, or if the landed envelope carries only *some* of `envelope`'s
    /// transaction ids — one id reached two committers, which the caller
    /// invariants forbid.
    pub async fn commit_slot(
        &self,
        sequence: u64,
        envelope: &Envelope,
    ) -> Result<CommitOutcome, Error> {
        let unknown = match self.put_slot(sequence, envelope).await {
            Ok(SlotRace::Won) => return Ok(CommitOutcome::Won),
            Ok(SlotRace::Lost) => None,
            Err(unknown) => Some(unknown),
        };

        match self.read_slot(sequence).await? {
            Some(landed) => match resolve(&landed, envelope.transaction_ids()) {
                Resolution::ThisAttempt => Ok(CommitOutcome::Won),
                Resolution::OtherEnvelope => Ok(CommitOutcome::Lost(landed)),
                Resolution::NoIdentity => Err(Error::transport(format!(
                    "slot {sequence} holds an envelope and this attempt carries no \
                     transaction id to match against it, so the put's outcome cannot \
                     be determined"
                ))),
                Resolution::PartlyLanded => Err(Error::corruption(format!(
                    "slot {sequence} carries some but not all of this attempt's \
                     transaction ids; an id reached more than one committer"
                ))),
            },
            None => Err(unknown.unwrap_or_else(|| {
                Error::transport(format!(
                    "slot {sequence} was reported taken but reads absent; a competing \
                     put may still be in flight"
                ))
            })),
        }
    }

    /// The tail from `from`: one LIST of the prefix (fixed-width names make
    /// lexicographic order numeric), then a GET per listed slot.
    ///
    /// Returns the contiguous run starting at `from`, and reports a hole in
    /// [`Tail::gap_at`] when the listing shows slots *above* an absent one.
    /// Slots below `from` are never inspected, so a truncated prefix reads as
    /// no hole.
    ///
    /// A `from` below 1 reads as 1: slot 0 never exists, so "from 0" can only
    /// mean "from the beginning". `from` must still sit at or above the
    /// truncation horizon, which only the caller knows — below it a deleted
    /// prefix and a destroyed slot are the same observation, and the tail reads
    /// as holed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing or a get failed, or
    /// [`Error::Corruption`] if a slot's bytes are not a valid envelope, or if
    /// a slot the listing showed reads absent.
    pub async fn read_tail(&self, from: u64) -> Result<Tail, Error> {
        let from = from.max(FIRST_SEQUENCE);
        let mut slots = Vec::new();
        let mut gap_at = None;

        let mut expected = from;
        for sequence in self.list_sequences(from).await? {
            if sequence != expected {
                gap_at = Some(expected);
                break;
            }

            let envelope = self.read_slot(sequence).await?.ok_or_else(|| {
                Error::corruption(format!(
                    "slot {sequence} was listed but reads absent; it was destroyed \
                     outside the protocol"
                ))
            })?;
            slots.push((sequence, envelope));
            expected = sequence.saturating_add(1);
        }

        Ok(Tail { slots, gap_at })
    }

    /// Deletes slots `..=through`, oldest first; already-missing slots count
    /// as deleted. Returns how many objects were removed. Choosing a safe
    /// `through` is the caller's policy, not this crate's.
    ///
    /// Enumerates from sequence 0, below the range the readers serve, so an
    /// object a writer never should have created is still reclaimable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing or a delete failed; the
    /// slots already deleted stay deleted, so a retry resumes.
    pub async fn truncate_through(&self, through: u64) -> Result<u64, Error> {
        let mut removed = 0;
        for sequence in self.list_sequences(0).await? {
            if sequence > through {
                break;
            }
            if self.delete_if_present(sequence).await? {
                removed += 1;
            }
        }

        Ok(removed)
    }

    /// How many contiguous slots are present from `from` — one LIST, no
    /// bodies fetched. The staleness signal.
    ///
    /// Counts the contiguous run only, so a hole ends the count and a damaged
    /// log reports a short tail: a caller that acts on staleness alone will
    /// see less work than exists, and only [`SlotLog::read_tail`] detects the
    /// hole. `from` is treated as in [`SlotLog::read_tail`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing failed.
    pub async fn tail_length(&self, from: u64) -> Result<u64, Error> {
        let from = from.max(FIRST_SEQUENCE);
        let mut length = 0;

        let mut expected = from;
        for sequence in self.list_sequences(from).await? {
            if sequence != expected {
                break;
            }
            length += 1;
            expected = sequence.saturating_add(1);
        }

        Ok(length)
    }

    /// How far the contiguous run from `from` reaches, and where the log
    /// continues past a hole — one LIST, no bodies fetched.
    ///
    /// `last` is the run's final sequence, `None` when the log holds nothing
    /// at `from`. `gap_at` carries the same meaning as
    /// [`Tail::gap_at`](Tail::gap_at): a sequence absent while higher ones are
    /// present. `from` is treated as in [`SlotLog::read_tail`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing failed.
    pub async fn tail_extent(&self, from: u64) -> Result<Extent, Error> {
        let from = from.max(FIRST_SEQUENCE);
        let mut extent = Extent {
            last: None,
            gap_at: None,
        };

        let mut expected = from;
        for sequence in self.list_sequences(from).await? {
            if sequence != expected {
                extent.gap_at = Some(expected);
                break;
            }
            extent.last = Some(sequence);
            expected = sequence.saturating_add(1);
        }

        Ok(extent)
    }

    /// Every slot present at or above `from`, ascending, with the wall-clock
    /// second each object was last written — one LIST, no bodies fetched.
    /// Sequences are contiguous only if the log is; a hole simply shows as a
    /// jump.
    ///
    /// The timestamp is the store's, not this crate's: nothing here reads a
    /// clock, and a policy that ages slots out supplies its own present.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing failed.
    pub async fn list_slots(&self, from: u64) -> Result<Vec<SlotMeta>, Error> {
        let listing = self.list_with_metadata(from).await?;
        let mut slots: Vec<SlotMeta> = listing
            .into_iter()
            .map(|(sequence, written_unix_seconds)| SlotMeta {
                sequence,
                written_unix_seconds,
            })
            .collect();
        slots.sort_unstable_by_key(|slot| slot.sequence);

        Ok(slots)
    }

    /// Scans the tail from `from` for a transaction id; the sequence of the
    /// slot that carries it, if any.
    ///
    /// A hole below the id refuses rather than answering: past a destroyed
    /// slot the scan cannot rule the transaction out. `from` is treated as in
    /// [`SlotLog::read_tail`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the tail read failed, or
    /// [`Error::Corruption`] on a malformed slot or a tail with a hole the
    /// id was not found below.
    pub async fn find_transaction(
        &self,
        from: u64,
        transaction_id: [u8; 16],
    ) -> Result<Option<u64>, Error> {
        let from = from.max(FIRST_SEQUENCE);
        let tail = self.read_tail(from).await?;
        let found = tail
            .slots
            .iter()
            .find(|(_, envelope)| envelope.contains_transaction(transaction_id))
            .map(|(sequence, _)| *sequence);

        match (found, tail.gap_at) {
            (Some(sequence), _) => Ok(Some(sequence)),
            (None, Some(gap)) => Err(Error::corruption(format!(
                "the tail from {from} has a hole at {gap}; a transaction cannot be \
                 ruled out past a destroyed slot"
            ))),
            (None, None) => Ok(None),
        }
    }

    /// Deletes one slot, reporting whether an object was actually removed.
    async fn delete_if_present(&self, sequence: u64) -> Result<bool, Error> {
        match self.store.delete(&self.slot_path(sequence)).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(Error::transport(format!("slot {sequence}: {err}"))),
        }
    }

    /// Every sequence present at or above `from`, ascending, from one LIST.
    async fn list_sequences(&self, from: u64) -> Result<Vec<u64>, Error> {
        let mut sequences: Vec<u64> = self
            .list_with_metadata(from)
            .await?
            .into_iter()
            .map(|(sequence, _)| sequence)
            .collect();
        sequences.sort_unstable();

        Ok(sequences)
    }

    /// Every sequence present at or above `from` with its object's last-write
    /// timestamp, in listing order, from one LIST. Only objects at a
    /// sequence's exact path count as slots, so anything else under the
    /// prefix — a nested key, a directory marker — is skipped.
    async fn list_with_metadata(&self, from: u64) -> Result<Vec<(u64, i64)>, Error> {
        // `list_with_offset` is exclusive, so offset by the predecessor's
        // name; below sequence 1 the prefix itself sorts before every slot.
        let offset = match from.checked_sub(1) {
            Some(previous) => self.slot_path(previous),
            None => self.prefix.clone(),
        };

        let listing: Vec<_> = self
            .store
            .list_with_offset(Some(&self.prefix), &offset)
            .try_collect()
            .await
            .map_err(|err| Error::transport(format!("listing slots from {from}: {err}")))?;

        Ok(listing
            .iter()
            .filter_map(|meta| {
                let sequence = parse_sequence(&meta.location)?;
                (sequence >= from && meta.location == self.slot_path(sequence))
                    .then(|| (sequence, meta.last_modified.timestamp()))
            })
            .collect())
    }
}

/// How far a contiguous run of slots reaches, and where the log continues
/// past a hole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// The run's final sequence; `None` when the run is empty.
    pub last: Option<u64>,
    /// A sequence absent while higher sequences are present.
    pub gap_at: Option<u64>,
}

/// One listed slot: its sequence and when its object was last written, in
/// seconds since the Unix epoch as the object store reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotMeta {
    /// The slot's sequence.
    pub sequence: u64,
    /// When the object was last written, per the store's own clock.
    pub written_unix_seconds: i64,
}

/// What a slot's landed envelope says about an attempt whose outcome is
/// unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resolution {
    /// Every one of the attempt's transaction ids is present: the attempt
    /// landed, whether alone or coalesced into a larger envelope.
    ThisAttempt,
    /// None of them is present: another committer holds the slot.
    OtherEnvelope,
    /// The attempt carries no transaction id, so nothing in the log
    /// distinguishes its own landed envelope from any other.
    NoIdentity,
    /// Some but not all are present, which takes one id reaching two
    /// committers. Forbidden by the caller invariants, so it is refused rather
    /// than reported as a win over transactions that never committed.
    PartlyLanded,
}

/// Compares a slot's landed envelope with the transaction ids one attempt
/// offered.
pub(crate) fn resolve<I>(landed: &Envelope, attempt_ids: I) -> Resolution
where
    I: IntoIterator<Item = [u8; 16]>,
{
    let mut offered = 0_usize;
    let mut present = 0_usize;
    for transaction_id in attempt_ids {
        offered += 1;
        if landed.contains_transaction(transaction_id) {
            present += 1;
        }
    }

    if offered == 0 {
        Resolution::NoIdentity
    } else if present == 0 {
        Resolution::OtherEnvelope
    } else if present < offered {
        Resolution::PartlyLanded
    } else {
        Resolution::ThisAttempt
    }
}

/// The sequence a slot object's path names, or `None` if it names no slot.
fn parse_sequence(location: &Path) -> Option<u64> {
    let name = location.parts().next_back()?;
    let name = name.as_ref();
    if name.len() != SEQUENCE_WIDTH || !name.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    name.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        envelope::{Commit, Envelope, SlotPayload, SlotWrite},
        fault::{FaultyPut, PutFault},
    };

    fn commit_with_id(transaction_id: [u8; 16]) -> Commit {
        Commit {
            transaction_id,
            payload: SlotPayload {
                validated_head: 0,
                changes_made: String::new(),
                writes: vec![],
            },
        }
    }

    fn envelope_with_id(transaction_id: [u8; 16]) -> Envelope {
        Envelope {
            leader: None,
            commits: vec![commit_with_id(transaction_id)],
        }
    }

    #[tokio::test]
    async fn racing_puts_admit_exactly_one_winner() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let log = SlotLog::new(store, "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        let first = log.put_slot(1, &envelope).await.unwrap();
        let second = log.put_slot(1, &envelope).await.unwrap();
        assert!(matches!(first, SlotRace::Won));
        assert!(matches!(second, SlotRace::Lost));
    }

    #[tokio::test]
    async fn slots_roundtrip_and_missing_reads_none() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        assert!(log.read_slot(1).await.unwrap().is_none());
        let envelope = Envelope {
            leader: None,
            commits: vec![Commit {
                transaction_id: [7; 16],
                payload: SlotPayload {
                    validated_head: 41,
                    changes_made: String::new(),
                    writes: vec![SlotWrite {
                        key: vec![1],
                        value: None,
                    }],
                },
            }],
        };
        log.put_slot(1, &envelope).await.unwrap();
        assert_eq!(log.read_slot(1).await.unwrap().unwrap(), envelope);
        assert!(
            log.read_slot(1)
                .await
                .unwrap()
                .unwrap()
                .contains_transaction([7; 16])
        );
    }

    #[test]
    fn sequence_names_are_fixed_width_and_ordered() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        assert_eq!(
            log.slot_path(42).as_ref(),
            "cat/commits/00000000000000000042"
        );
        assert!(log.slot_path(9).as_ref() < log.slot_path(10).as_ref());
    }

    /// An empty root leaves the slots directly under `commits/`, with no
    /// leading delimiter.
    #[test]
    fn an_empty_root_needs_no_leading_delimiter() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "");
        assert_eq!(log.slot_path(1).as_ref(), "commits/00000000000000000001");
    }

    #[tokio::test]
    async fn a_lost_race_returns_the_winning_envelope() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let winner = envelope_with_id([1; 16]);
        let loser = envelope_with_id([2; 16]);
        assert!(matches!(
            log.commit_slot(1, &winner).await.unwrap(),
            CommitOutcome::Won
        ));
        match log.commit_slot(1, &loser).await.unwrap() {
            CommitOutcome::Lost(read_back) => assert_eq!(read_back, winner),
            CommitOutcome::Won => unreachable!("slot 1 was taken"),
        }
    }

    #[tokio::test]
    async fn read_tail_stops_at_the_first_absent_slot_and_reports_a_hole() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        for sequence in [1, 2, 4] {
            log.put_slot(sequence, &envelope).await.unwrap();
        }
        // Slot 3 is missing while 4 exists: a destroyed slot, not an end.
        let tail = log.read_tail(1).await.unwrap();
        assert_eq!(
            tail.slots.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(tail.gap_at, Some(3));

        // A clean end of log reports no hole.
        let clean = SlotLog::new(Arc::new(InMemory::new()), "cat");
        clean.put_slot(1, &envelope).await.unwrap();
        let tail = clean.read_tail(1).await.unwrap();
        assert_eq!(tail.slots.len(), 1);
        assert_eq!(tail.gap_at, None);
        assert!(clean.read_tail(5).await.unwrap().slots.is_empty());
    }

    #[tokio::test]
    async fn a_truncated_prefix_is_not_a_hole() {
        // Truncation deletes a prefix; reading from above it sees no gap.
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        for sequence in 1..=4 {
            log.put_slot(sequence, &envelope).await.unwrap();
        }
        log.truncate_through(2).await.unwrap();
        let tail = log.read_tail(3).await.unwrap();
        assert_eq!(tail.slots.len(), 2);
        assert_eq!(tail.gap_at, None);
    }

    #[tokio::test]
    async fn truncate_through_deletes_oldest_first_and_tolerates_gaps() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        for sequence in [1, 3, 4] {
            log.put_slot(sequence, &envelope).await.unwrap();
        }
        assert_eq!(log.truncate_through(3).await.unwrap(), 2); // 1 and 3; 2 already gone
        assert!(log.read_slot(1).await.unwrap().is_none());
        assert!(log.read_slot(4).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_length_counts_without_bodies_and_find_transaction_scans() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        for (sequence, id) in [(1, [1; 16]), (2, [2; 16])] {
            log.put_slot(sequence, &envelope_with_id(id)).await.unwrap();
        }
        assert_eq!(log.tail_length(1).await.unwrap(), 2);
        assert_eq!(log.tail_length(3).await.unwrap(), 0);
        assert_eq!(log.find_transaction(1, [2; 16]).await.unwrap(), Some(2));
        assert_eq!(log.find_transaction(1, [9; 16]).await.unwrap(), None);
    }

    /// A hole means the scan cannot rule the transaction out, so it refuses
    /// rather than reporting a transaction that may have committed as
    /// absent.
    #[tokio::test]
    async fn find_transaction_refuses_a_tail_with_a_hole() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        for (sequence, id) in [(1_u64, [1_u8; 16]), (3, [3; 16])] {
            log.put_slot(sequence, &envelope_with_id(id)).await.unwrap();
        }
        let err = log.find_transaction(1, [3; 16]).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        // The slots below the hole still answer.
        assert_eq!(log.find_transaction(1, [1; 16]).await.unwrap(), Some(1));
    }

    /// A store reporting a slot taken is not proof this attempt's put failed:
    /// a retry below this crate can land a create and then hear the store call
    /// the object already present. An attempt re-racing a sequence its own
    /// earlier put may have won is told it holds it, not handed its own
    /// envelope as a rival's.
    #[tokio::test]
    async fn a_re_raced_slot_resolves_to_this_attempts_own_envelope() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = envelope_with_id([9; 16]);
        log.put_slot(1, &envelope).await.unwrap();

        assert_eq!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        );
        // A coalescing winner that carries the attempt is the same answer.
        let coalesced = Envelope {
            leader: None,
            commits: vec![commit_with_id([8; 16]), commit_with_id([9; 16])],
        };
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        log.put_slot(1, &coalesced).await.unwrap();
        assert_eq!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        );
    }

    /// An id-less envelope cannot be told from another committer's in either
    /// direction, so a taken slot is retryable rather than a loss. Retrying
    /// costs nothing: such an envelope carries no writes.
    #[tokio::test]
    async fn a_taken_slot_cannot_be_attributed_for_an_id_less_attempt() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        log.put_slot(1, &envelope_with_id([1; 16])).await.unwrap();

        let err = log
            .commit_slot(
                1,
                &Envelope {
                    leader: None,
                    commits: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Transport(_)), "{err}");
    }

    /// The exactly-once mechanic: a put that landed under a lost response
    /// is resolved from the log by transaction id, not guessed.
    #[tokio::test]
    async fn an_ambiguous_put_that_landed_resolves_to_won() {
        let log = SlotLog::new(Arc::new(FaultyPut::armed(PutFault::LostResponse)), "cat");
        let envelope = envelope_with_id([3; 16]);
        assert!(matches!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        ));
        assert_eq!(log.read_slot(1).await.unwrap().unwrap(), envelope);
    }

    /// An ambiguous put whose object is absent on read-back genuinely did
    /// not land, so the transport error surfaces — and the same sequence is
    /// still there to win on the retry.
    #[tokio::test]
    async fn an_ambiguous_put_that_never_landed_surfaces_the_transport_error() {
        let store = Arc::new(FaultyPut::armed(PutFault::Unreachable));
        let log = SlotLog::new(store.clone(), "cat");
        let envelope = envelope_with_id([4; 16]);

        let err = log.commit_slot(1, &envelope).await.unwrap_err();
        assert!(matches!(err, Error::Transport(_)), "{err}");
        assert!(log.read_slot(1).await.unwrap().is_none());

        store.disarm();
        assert!(matches!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        ));
    }

    /// A store may report a slot taken before the winner's object is
    /// readable — real S3 answers 409 to a create whose competitor is still
    /// in flight. That is an ordinary contended round on a healthy log, so it
    /// must be retryable, never [`Error::Corruption`].
    #[tokio::test]
    async fn a_slot_reported_taken_but_absent_is_retryable_not_corrupt() {
        let store = Arc::new(FaultyPut::armed(PutFault::PrematureAlreadyExists));
        let log = SlotLog::new(store.clone(), "cat");
        let envelope = envelope_with_id([5; 16]);

        let err = log.commit_slot(1, &envelope).await.unwrap_err();
        assert!(matches!(err, Error::Transport(_)), "{err}");
        assert!(log.read_slot(1).await.unwrap().is_none());

        store.disarm();
        assert!(matches!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        ));
    }

    /// An envelope with no commits has no identity, so an ambiguous put of one
    /// is unresolvable however the slot reads back. Reporting `Won` would tell
    /// a committer it owns a sequence another committer may own.
    #[tokio::test]
    async fn an_ambiguous_put_with_no_transaction_id_is_unresolvable() {
        let log = SlotLog::new(Arc::new(FaultyPut::armed(PutFault::LostResponse)), "cat");
        let err = log
            .commit_slot(
                1,
                &Envelope {
                    leader: None,
                    commits: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Transport(_)), "{err}");
    }

    /// Two committers put structurally identical id-less envelopes: A's lands,
    /// B's never reaches the store. B must not be told it won the slot A owns —
    /// byte equality cannot tell the two envelopes apart.
    #[tokio::test]
    async fn a_loser_with_no_transaction_id_is_never_told_it_won() {
        let store = Arc::new(FaultyPut::disarmed(PutFault::Unreachable));
        let log = SlotLog::new(store.clone(), "cat");
        let advert = Envelope {
            leader: None,
            commits: vec![],
        };

        // Committer A wins slot 1.
        assert_eq!(
            log.commit_slot(1, &advert).await.unwrap(),
            CommitOutcome::Won
        );

        // Committer B's put never reaches the store, so it wrote nothing.
        store.arm();
        let outcome = log.commit_slot(1, &advert).await;
        assert!(
            matches!(outcome, Err(Error::Transport(_))),
            "an id-less loser must not hear Won: {outcome:?}"
        );

        // A's envelope is untouched and still the only one at slot 1.
        assert_eq!(log.read_slot(1).await.unwrap().unwrap(), advert);
    }

    /// A landed envelope holding only *some* of an attempt's ids takes one id
    /// reaching two committers, which the caller invariants forbid. Refused,
    /// rather than reported as a win over the transactions that never landed.
    #[tokio::test]
    async fn a_partly_landed_attempt_is_refused() {
        let store = Arc::new(FaultyPut::disarmed(PutFault::Unreachable));
        let log = SlotLog::new(store.clone(), "cat");

        // Slot 1 ends up holding transaction 6 alone.
        log.commit_slot(1, &envelope_with_id([6; 16]))
            .await
            .unwrap();

        // An attempt over {6, 7} whose put fails finds only 6 present.
        store.arm();
        let attempt = Envelope {
            leader: None,
            commits: vec![commit_with_id([6; 16]), commit_with_id([7; 16])],
        };
        let err = log.commit_slot(1, &attempt).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    /// A superset is a win: a leader that coalesced this commit into a larger
    /// envelope did commit this attempt's transaction.
    #[tokio::test]
    async fn a_coalesced_superset_still_resolves_to_won() {
        let store = Arc::new(FaultyPut::disarmed(PutFault::Unreachable));
        let log = SlotLog::new(store.clone(), "cat");

        let coalesced = Envelope {
            leader: None,
            commits: vec![
                commit_with_id([6; 16]),
                commit_with_id([7; 16]),
                commit_with_id([8; 16]),
            ],
        };
        log.commit_slot(1, &coalesced).await.unwrap();

        store.arm();
        let attempt = Envelope {
            leader: None,
            commits: vec![commit_with_id([7; 16]), commit_with_id([8; 16])],
        };
        assert_eq!(
            log.commit_slot(1, &attempt).await.unwrap(),
            CommitOutcome::Won
        );
    }

    /// A put that failed with an unknown outcome onto a slot another envelope
    /// already holds resolves to `Lost` carrying that envelope — the same
    /// answer the `AlreadyExists` path gives, reached the other way.
    #[tokio::test]
    async fn an_unknown_put_onto_a_taken_slot_resolves_to_lost() {
        let store = Arc::new(FaultyPut::disarmed(PutFault::Unreachable));
        let log = SlotLog::new(store.clone(), "cat");

        let winner = envelope_with_id([1; 16]);
        log.commit_slot(1, &winner).await.unwrap();

        store.arm();
        let outcome = log
            .commit_slot(1, &envelope_with_id([2; 16]))
            .await
            .unwrap();
        assert_eq!(outcome, CommitOutcome::Lost(winner));
    }

    /// Resolution matches *any* of the attempt's ids, so a multi-commit
    /// envelope resolves on whichever it finds.
    #[tokio::test]
    async fn an_ambiguous_put_matches_any_of_a_multi_commit_envelopes_ids() {
        let log = SlotLog::new(Arc::new(FaultyPut::armed(PutFault::LostResponse)), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![commit_with_id([6; 16]), commit_with_id([7; 16])],
        };

        assert_eq!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        );
        let landed = log.read_slot(1).await.unwrap().unwrap();
        assert!(landed.contains_transaction([6; 16]));
        assert!(landed.contains_transaction([7; 16]));
    }

    /// Store text reaches an embedder's retry loop through this crate's
    /// error, so the substrings that loop keys on never survive the wrapping.
    #[tokio::test]
    async fn transport_errors_carry_no_retry_substrings_from_the_store() {
        let log = SlotLog::new(Arc::new(FaultyPut::armed(PutFault::Unreachable)), "cat");
        let err = log
            .put_slot(1, &envelope_with_id([8; 16]))
            .await
            .unwrap_err();

        let text = err.to_string().to_ascii_lowercase();
        for substring in ["conflict", "concurrent", "unique", "primary key"] {
            assert!(!text.contains(substring), "{text:?} contains {substring:?}");
        }
    }

    /// Slot 0 never exists, so a `from` below the first sequence means "from
    /// the beginning" rather than a hole at 0. A `from` below the truncation
    /// horizon still reads as holed — only the caller knows that horizon.
    #[tokio::test]
    async fn a_from_below_the_first_sequence_reads_from_the_beginning() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        for sequence in 1..=4 {
            log.put_slot(sequence, &envelope).await.unwrap();
        }

        let tail = log.read_tail(0).await.unwrap();
        assert_eq!(
            tail.slots.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(tail.gap_at, None);
        assert_eq!(log.tail_length(0).await.unwrap(), 4);
        assert_eq!(log.find_transaction(0, [1; 16]).await.unwrap(), None);

        // Below the truncation horizon, a deleted prefix reads as holed.
        log.truncate_through(2).await.unwrap();
        assert_eq!(log.read_tail(1).await.unwrap().gap_at, Some(1));
        assert_eq!(log.tail_length(1).await.unwrap(), 0);
    }

    /// Truncation reaches below the range reads serve, so an object a writer
    /// never should have created at sequence 0 is still reclaimable.
    #[tokio::test]
    async fn truncation_reclaims_a_slot_at_sequence_zero() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        for sequence in [0, 1, 2] {
            log.put_slot(sequence, &envelope).await.unwrap();
        }

        assert_eq!(log.truncate_through(1).await.unwrap(), 2);
        assert!(log.read_slot(0).await.unwrap().is_none());
        assert!(log.read_slot(1).await.unwrap().is_none());
        assert!(log.read_slot(2).await.unwrap().is_some());
    }

    /// `list` is recursive, so an object *nested under* a slot's name must not
    /// be mistaken for a slot — including when its own final segment is a
    /// well-formed sequence name.
    #[tokio::test]
    async fn a_nested_sequence_shaped_object_is_not_a_slot() {
        let store = Arc::new(InMemory::new());
        let log = SlotLog::new(store.clone(), "cat");
        let envelope = Envelope {
            leader: None,
            commits: vec![],
        };
        for sequence in [1, 2] {
            log.put_slot(sequence, &envelope).await.unwrap();
        }

        // Final segment parses as sequence 5, but the object does not live at
        // slot 5's path.
        let nested = log.slot_path(2).join(format!("{:020}", 5));
        store.put(&nested, "not a slot".into()).await.unwrap();

        // Counted as a slot, 5 would break the run after 2 and report a hole.
        let tail = log.read_tail(1).await.unwrap();
        assert_eq!(
            tail.slots.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(tail.gap_at, None);
        assert_eq!(log.tail_length(1).await.unwrap(), 2);
    }
}
