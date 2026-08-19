//! A commit log that lives in a bucket: totally ordered, immutable slots
//! under `<root>/commits/`, one object per sequence, each written exactly
//! once with a create-if-absent conditional put.
//!
//! The conditional put is the entire arbitration mechanism. Exactly one
//! committer can win each sequence, however many race it — no lock, no
//! lease, no coordinator. Everything above that is optimization.
//!
//! This crate owns the *shape* of the protocol: sequence naming, the
//! envelope wire format, winning and losing a race, resolving a put whose
//! outcome is unknown, enumerating and truncating the tail. It owns none of
//! the *meaning*: a payload's keys and values are opaque bytes, and its
//! [`changes_made`](SlotPayload::changes_made) classification is opaque
//! text. What they signify belongs to whatever embeds the log.
//!
//! # A worked example
//!
//! Win a slot, lose the next race to another committer, then replay the
//! tail into a byte-level view:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine_wal::{Commit, CommitOutcome, Envelope, Overlay, SlotLog, SlotPayload, SlotWrite};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let log = SlotLog::new(Arc::new(InMemory::new()), "catalog");
//!
//! // One commit: a batch of writes, the head it was validated against, and
//! // a committer-minted id.
//! let commit = |id: u8, value: &[u8]| Envelope { leader: None,
//!     commits: vec![Commit {
//!         transaction_id: [id; 16],
//!         payload: SlotPayload {
//!             validated_head: 0,
//!             changes_made: "created_table".to_string(),
//!             writes: vec![SlotWrite {
//!                 key: b"orders".to_vec(),
//!                 value: Some(value.to_vec()),
//!             }],
//!         },
//!     }],
//! };
//!
//! // The first committer to put slot 1 wins it.
//! let won = log.commit_slot(1, &commit(1, b"first")).await?;
//! assert_eq!(won, CommitOutcome::Won);
//!
//! // A second committer racing the same slot loses, and gets the winning
//! // envelope back to rebase onto.
//! let lost = log.commit_slot(1, &commit(2, b"second")).await?;
//! match &lost {
//!     CommitOutcome::Lost(winner) => assert!(winner.contains_transaction([1; 16])),
//!     CommitOutcome::Won => panic!("slot 1 was already taken"),
//! }
//!
//! // Rebased onto slot 2, it wins.
//! log.commit_slot(2, &commit(2, b"second")).await?;
//!
//! // Replay: the contiguous run from sequence 1, folded last-writer-wins.
//! let tail = log.read_tail(1).await?;
//! assert_eq!(tail.slots.len(), 2);
//! assert_eq!(tail.gap_at, None);
//!
//! let mut overlay = Overlay::default();
//! for (_, envelope) in &tail.slots {
//!     overlay.absorb(envelope);
//! }
//! assert_eq!(overlay.get(b"orders"), Some(Some(b"second".as_slice())));
//! # Ok::<(), moraine_wal::Error>(()) }).unwrap();
//! ```
//!
//! # Two mechanics worth knowing before you embed this
//!
//! **A put that did not plainly win resolves from the log, never by
//! guessing.** A conditional put whose *response* is lost is
//! indistinguishable, from the caller's side, from one that never landed — and
//! a store reporting the sequence already taken is no proof of a loss either,
//! since the retries below a put can land a create and then hear the object
//! called present. Both pose one question, and [`SlotLog::commit_slot`] answers
//! it by re-reading the slot: an envelope carrying the attempt's transaction
//! ids means this commit holds the sequence; an envelope carrying none of them
//! means it lost; an absent slot means the put did not land, and the transport
//! error surfaces. A retry that re-offers its transaction ids is therefore told
//! it already won, rather than handed its own envelope as a rival's.
//!
//! **A hole is damage, not an ending.** [`SlotLog::read_tail`] serves the
//! contiguous run from the requested sequence, but a sequence absent while
//! *higher* sequences exist means a slot was destroyed outside the protocol.
//! It is reported as [`Tail::gap_at`], and an embedder must refuse: serving
//! the prefix would hide committed state and let the next committer re-win
//! that sequence, forking history behind every process still replaying.
//! Sequences *below* the requested one are never inspected, so a
//! legitimately truncated prefix reads as no hole.
//!
//! # What the caller owns
//!
//! **Transaction ids are the identity resolution runs on.** Each must be
//! unique across every committer sharing a log, and each must be offered to
//! one committer only. Break the first and a reused id resolves another
//! committer's envelope as this attempt's; break the second and a slot can
//! carry part of an attempt, which [`SlotLog::commit_slot`] refuses as
//! corruption rather than report as a win over transactions that never
//! committed.
//!
//! **An envelope with no commits has no identity, so a taken slot is not
//! resolvable for it.** Such envelopes are accepted — they carry no writes, and
//! a reserved advert field will ride them — but nothing in the log
//! distinguishes this attempt's landed envelope from an identical one another
//! committer put, in either direction. [`SlotLog::commit_slot`] reports that as
//! a retryable failure rather than guessing, so a caller that wants the winner
//! of a contended sequence reads the slot itself. Retrying is safe: the
//! envelope carries no writes, so a duplicate changes no state.
//!
//! **`from` must sit at or above the truncation horizon**, which only the
//! caller knows. Below it a deleted prefix and a destroyed slot are the same
//! observation, and the tail reads as holed. A fold cursor of `n` means slots
//! `1..=n` are applied, so the next tail starts at `n + 1`. A `from` below 1
//! is raised to 1, since slot 0 never exists.
//!
//! # Layering
//!
//! - `slot` — the log: sequence naming, the race, the tail, truncation.
//! - `envelope` — a slot's content, its codec, and the overlay a tail folds
//!   into.
//! - `frame` — the magic and encoding version every slot object opens with.
//! - `driver` — the loops over the log: the retry/rebase commit round, the fold
//!   round, and the folder appointment rule, each generic over the embedder's
//!   semantics.
//! - `engine` — the log presented as SlateDB's write-ahead log, so a store
//!   folds it by replaying it and a reader serves its unfolded tail. The only
//!   module that names a storage engine; payload keys and values stay opaque
//!   even there.
//!
//! The crate spawns no threads and schedules nothing: every call is one
//! bounded piece of work the host drives. The log's own safety is
//! sequence-ordinal, never temporal — it reads no clock. The drivers do wait
//! between attempts, and they draw the jitter in those waits from an
//! explicitly seeded [`Jitter`], so a run reproduces from its seed.
//!
//! # The loops, and what an embedder plugs into them
//!
//! [`drive_commit`] assembles, races, and rebases under a
//! [`RetryPolicy`] budget over a [`Committer`]; [`drive_fold`] applies
//! unapplied slots into a [`CursorStore`], resuming from its cursor; and
//! [`drive_fold_if_stalled`] decides *when* a would-be folder should acquire
//! its [`FolderRole`] at all. The embedder supplies commit semantics and
//! derived state; the loops supply sequencing, backoff, budgets, resume, and
//! the stand-down rule.
//!
//! # Folding without a fold loop
//!
//! [`SlotWal`] hands the same job to SlateDB instead: plugged in as a store's
//! write-ahead log, opening the store replays the slots past its cursor — that
//! replay *is* the fold — and the store's own replay point is the cursor.
//! Readers plugged in the same way serve the unfolded tail without an
//! [`Overlay`] of their own, and [`WalGc`](SlotWal) truncation follows the
//! ranges live manifests still reference.
//!
//! A store folding this way must take no writes of its own: SlateDB draws row
//! sequence numbers and slot ordinals from one space, so a direct write pushes
//! the store's counter past the ordinals of slots not yet folded, and replay
//! skips them. [`SlotWal`]'s writer refuses appends for the same reason.

#![forbid(unsafe_code)]

mod driver;
mod engine;
mod envelope;
mod error;
#[cfg(test)]
mod fault;
mod frame;
mod proto;
mod slot;

pub use driver::{
    CommitDrive, Committer, CursorStore, FoldReport, FolderRole, Jitter, Race, RetryPolicy,
    drive_commit, drive_fold, drive_fold_if_stalled,
};
pub use engine::{AdvertProjection, AppendRefused, SlotWal};
pub use envelope::{Commit, Envelope, LeaderAdvert, Overlay, SlotPayload, SlotWrite};
pub use error::Error;
pub use proto::FoldValue;
pub use slot::{CommitOutcome, Extent, SlotLog, SlotMeta, SlotRace, Tail};
