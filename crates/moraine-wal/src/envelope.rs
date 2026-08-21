//! One slot's content, its wire codec, and the byte-level view a tail folds
//! into. Payload keys, payload values, and the classification string are
//! opaque bytes and opaque text here — their meaning is the embedder's.

use std::collections::BTreeMap;

use prost::Message;

use crate::{
    error::Error,
    frame,
    proto::{CommitValue, EnvelopeValue, LeaderAdvertValue, SlotPayloadValue, SlotWriteValue},
};

/// The width of a committer-minted transaction id.
const TRANSACTION_ID_LEN: usize = 16;

/// The width of a leader instance id.
const INSTANCE_ID_LEN: usize = 16;

/// One slot's content: the commits that won it, in order, and an optional
/// leader advert riding along.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// The commits this slot carries, in the order they apply.
    pub commits: Vec<Commit>,
    /// A leader announcement riding this slot, if any. An advert whose
    /// `endpoint` is absent withdraws the role; an absent advert says nothing.
    pub leader: Option<LeaderAdvert>,
}

/// A leader announcement riding a slot. `endpoint` present announces the role
/// at that address; `endpoint` absent **withdraws** it — a state an absent
/// advert cannot express.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderAdvert {
    /// The announcing leader's instance id, distinguishing successive holders.
    pub instance: [u8; INSTANCE_ID_LEN],
    /// The address to forward to, or `None` to withdraw.
    pub endpoint: Option<String>,
}

/// One committed unit under a committer-minted id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The committer-minted id of this commit. Must be unique across every
    /// committer sharing the log: an ambiguous put is resolved by matching
    /// it, so a reused id resolves another committer's envelope as this one.
    pub transaction_id: [u8; TRANSACTION_ID_LEN],
    /// What this commit writes, and what it was validated against.
    pub payload: SlotPayload,
}

/// A commit's content: a batch of key-value writes plus what it was
/// validated against. Keys, values, and the classification string are
/// opaque here — their meaning is the embedder's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPayload {
    /// The head version this change set was validated against.
    pub validated_head: u64,
    /// Embedder-defined classification input for lost-race judgment.
    pub changes_made: String,
    /// The writes this commit stages, in the order they apply.
    pub writes: Vec<SlotWrite>,
}

/// One staged write: an absent value is a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotWrite {
    /// The key written, opaque bytes.
    pub key: Vec<u8>,
    /// The value written, or `None` to delete the key.
    pub value: Option<Vec<u8>>,
}

impl Envelope {
    /// An envelope of `commits` carrying no advert.
    #[must_use]
    pub fn new(commits: Vec<Commit>) -> Self {
        Self {
            commits,
            leader: None,
        }
    }

    /// This envelope with `advert` riding it.
    #[must_use]
    pub fn with_advert(mut self, advert: LeaderAdvert) -> Self {
        self.leader = Some(advert);
        self
    }

    /// Whether any commit in this envelope carries `transaction_id`.
    #[must_use]
    pub fn contains_transaction(&self, transaction_id: [u8; TRANSACTION_ID_LEN]) -> bool {
        self.commits
            .iter()
            .any(|commit| commit.transaction_id == transaction_id)
    }

    /// The transaction ids this envelope carries, in commit order.
    pub(crate) fn transaction_ids(&self) -> impl Iterator<Item = [u8; TRANSACTION_ID_LEN]> + '_ {
        self.commits.iter().map(|commit| commit.transaction_id)
    }

    /// This envelope's framed wire bytes: the exact object body a slot put
    /// writes.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let wire = EnvelopeValue {
            commits: self.commits.iter().map(Commit::to_wire).collect(),
            leader: self.leader.as_ref().map(LeaderAdvert::to_wire),
        };

        let mut framed = frame::header_buf(wire.encoded_len());
        // Infallible by construction: insufficient capacity is the only
        // error `encode` returns, and a `Vec`'s `BufMut` capacity is
        // unbounded.
        #[allow(clippy::expect_used)]
        wire.encode(&mut framed)
            .expect("prost encode into a Vec cannot fail");

        framed
    }

    /// Decodes a slot object's bytes. Any framing, protobuf, or structural
    /// failure is [`Error::Corruption`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let payload = frame::unframe(bytes)?;
        let wire = EnvelopeValue::decode(payload)
            .map_err(|err| Error::corruption(format!("slot: {err}")))?;

        Ok(Self {
            commits: wire
                .commits
                .into_iter()
                .map(Commit::from_wire)
                .collect::<Result<_, _>>()?,
            leader: wire.leader.map(LeaderAdvert::from_wire).transpose()?,
        })
    }
}

impl LeaderAdvert {
    fn to_wire(&self) -> LeaderAdvertValue {
        LeaderAdvertValue {
            instance: self.instance.to_vec(),
            endpoint: self.endpoint.clone(),
        }
    }

    fn from_wire(wire: LeaderAdvertValue) -> Result<Self, Error> {
        let instance =
            <[u8; INSTANCE_ID_LEN]>::try_from(wire.instance.as_slice()).map_err(|_| {
                Error::corruption(format!(
                    "slot: leader instance id is {} bytes, expected {INSTANCE_ID_LEN}",
                    wire.instance.len()
                ))
            })?;

        Ok(Self {
            instance,
            endpoint: wire.endpoint,
        })
    }
}

impl Commit {
    fn to_wire(&self) -> CommitValue {
        CommitValue {
            transaction_id: self.transaction_id.to_vec(),
            payload: Some(self.payload.to_wire()),
        }
    }

    fn from_wire(wire: CommitValue) -> Result<Self, Error> {
        let transaction_id = <[u8; TRANSACTION_ID_LEN]>::try_from(wire.transaction_id.as_slice())
            .map_err(|_| {
            Error::corruption(format!(
                "slot: transaction id is {} bytes, expected {TRANSACTION_ID_LEN}",
                wire.transaction_id.len()
            ))
        })?;
        let payload = wire
            .payload
            .ok_or_else(|| Error::corruption("slot: commit carries no payload".to_string()))?;

        Ok(Self {
            transaction_id,
            payload: SlotPayload::from_wire(payload),
        })
    }
}

impl SlotPayload {
    fn to_wire(&self) -> SlotPayloadValue {
        SlotPayloadValue {
            validated_head: self.validated_head,
            changes_made: self.changes_made.clone(),
            writes: self
                .writes
                .iter()
                .map(|write| SlotWriteValue {
                    key: write.key.clone(),
                    value: write.value.clone(),
                })
                .collect(),
        }
    }

    fn from_wire(wire: SlotPayloadValue) -> Self {
        Self {
            validated_head: wire.validated_head,
            changes_made: wire.changes_made,
            writes: wire
                .writes
                .into_iter()
                .map(|write| SlotWrite {
                    key: write.key,
                    value: write.value,
                })
                .collect(),
        }
    }
}

/// A byte-level view of a tail: every write in slot order,
/// last-writer-wins, queryable by key. What the keys mean is the
/// embedder's business.
#[derive(Debug, Default, Clone)]
pub struct Overlay {
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Overlay {
    /// Folds an envelope's writes in, in order.
    pub fn absorb(&mut self, envelope: &Envelope) {
        for commit in &envelope.commits {
            for write in &commit.payload.writes {
                self.writes.insert(write.key.clone(), write.value.clone());
            }
        }
    }

    /// Roughly what the held writes occupy, keys and values together. A
    /// delete counts its key alone.
    #[must_use]
    pub fn estimated_bytes(&self) -> u64 {
        self.writes
            .iter()
            .map(|(key, value)| {
                let held = key.len() + value.as_ref().map_or(0, Vec::len);
                u64::try_from(held).unwrap_or(u64::MAX)
            })
            .fold(0_u64, u64::saturating_add)
    }

    /// `Some(Some(bytes))` = written, `Some(None)` = deleted, `None` =
    /// untouched by the tail.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.writes.get(key).map(Option::as_deref)
    }

    /// Every write whose key starts with `prefix`, ascending by key, each
    /// three-state as in [`get`](Self::get).
    pub fn prefixed<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)> {
        self.writes
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.as_slice(), value.as_deref()))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost::Message;

    use super::*;
    use crate::proto::{CommitValue, EnvelopeValue, SlotPayloadValue};

    fn envelope_writing(writes: &[(&[u8], Option<&[u8]>)]) -> Envelope {
        Envelope::new(vec![Commit {
            transaction_id: [0; 16],
            payload: SlotPayload {
                validated_head: 0,
                changes_made: String::new(),
                writes: writes
                    .iter()
                    .map(|(key, value)| SlotWrite {
                        key: (*key).to_vec(),
                        value: value.map(<[u8]>::to_vec),
                    })
                    .collect(),
            },
        }])
    }

    #[test]
    fn overlay_is_last_writer_wins_and_three_state() {
        let mut overlay = Overlay::default();
        overlay.absorb(&envelope_writing(&[(b"k", Some(b"old"))]));
        overlay.absorb(&envelope_writing(&[(b"k", Some(b"new")), (b"gone", None)]));
        assert_eq!(overlay.get(b"k"), Some(Some(b"new".as_slice())));
        assert_eq!(overlay.get(b"gone"), Some(None));
        assert_eq!(overlay.get(b"untouched"), None);
    }

    /// A prefix walk yields the tail's writes in key order, deletes included,
    /// and stops at the prefix — an embedder merges it into an ordered scan of
    /// its own store in one pass.
    #[test]
    fn a_prefix_walk_is_ordered_and_bounded_to_the_prefix() {
        let mut overlay = Overlay::default();
        overlay.absorb(&envelope_writing(&[
            (b"index/b", Some(b"2")),
            (b"index/a", Some(b"1")),
            (b"index/c", None),
            (b"other", Some(b"x")),
        ]));

        assert_eq!(
            overlay.prefixed(b"index/").collect::<Vec<_>>(),
            vec![
                (b"index/a".as_slice(), Some(b"1".as_slice())),
                (b"index/b".as_slice(), Some(b"2".as_slice())),
                (b"index/c".as_slice(), None),
            ]
        );
        assert_eq!(overlay.prefixed(b"absent").count(), 0);
        assert_eq!(overlay.prefixed(b"").count(), 4);
    }

    /// Transaction ids are fixed-width envelope structure: a slot whose id
    /// is the wrong width cannot be matched against an attempt's ids, so
    /// decoding refuses it.
    #[test]
    fn decode_rejects_a_wrong_width_transaction_id() {
        let wire = EnvelopeValue {
            commits: vec![CommitValue {
                transaction_id: vec![1; 15],
                payload: Some(SlotPayloadValue::default()),
            }],
            leader: None,
        };
        let err = Envelope::decode(&framed(&wire)).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    #[test]
    fn decode_rejects_a_commit_without_a_payload() {
        let wire = EnvelopeValue {
            commits: vec![CommitValue {
                transaction_id: vec![1; 16],
                payload: None,
            }],
            leader: None,
        };
        let err = Envelope::decode(&framed(&wire)).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    #[test]
    fn decode_rejects_a_wrong_width_instance_id() {
        let wire = EnvelopeValue {
            commits: vec![],
            leader: Some(crate::proto::LeaderAdvertValue {
                instance: vec![7; 15],
                endpoint: Some("host:1".to_string()),
            }),
        };
        let err = Envelope::decode(&framed(&wire)).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    /// An announcement, a withdrawal, and no advert are three distinct wire
    /// forms: an absent advert says nothing about leadership, an advert with an
    /// endpoint announces, and one without withdraws.
    #[test]
    fn announcement_withdrawal_and_no_advert_are_distinct() {
        let announce = Envelope::new(vec![]).with_advert(LeaderAdvert {
            instance: [3; 16],
            endpoint: Some("10.0.0.1:7000".to_string()),
        });
        let withdraw = Envelope::new(vec![]).with_advert(LeaderAdvert {
            instance: [3; 16],
            endpoint: None,
        });
        let silent = Envelope::new(vec![]);

        for envelope in [&announce, &withdraw, &silent] {
            assert_eq!(&Envelope::decode(&envelope.encode()).unwrap(), envelope);
        }
        assert_ne!(announce.encode(), withdraw.encode());
        assert_ne!(withdraw.encode(), silent.encode());
        assert_ne!(announce.encode(), silent.encode());

        assert!(matches!(
            Envelope::decode(&announce.encode()).unwrap().leader,
            Some(LeaderAdvert {
                endpoint: Some(_),
                ..
            })
        ));
        assert!(matches!(
            Envelope::decode(&withdraw.encode()).unwrap().leader,
            Some(LeaderAdvert { endpoint: None, .. })
        ));
        assert!(Envelope::decode(&silent.encode()).unwrap().leader.is_none());
    }

    #[test]
    fn decode_rejects_unframed_bytes() {
        let err = Envelope::decode(b"not framed").unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    /// Frames a wire message by hand, so a test can build envelopes the
    /// public types cannot express.
    fn framed(wire: &EnvelopeValue) -> Vec<u8> {
        let mut bytes = frame::header_buf(wire.encoded_len());
        wire.encode(&mut bytes).unwrap();
        bytes
    }

    fn any_advert() -> impl Strategy<Value = Option<LeaderAdvert>> {
        proptest::option::of(
            (any::<[u8; 16]>(), proptest::option::of(".*"))
                .prop_map(|(instance, endpoint)| LeaderAdvert { instance, endpoint }),
        )
    }

    fn any_envelope() -> impl Strategy<Value = Envelope> {
        let write = (
            proptest::collection::vec(any::<u8>(), 0..24),
            proptest::option::of(proptest::collection::vec(any::<u8>(), 0..24)),
        )
            .prop_map(|(key, value)| SlotWrite { key, value });
        let commit = (
            any::<[u8; 16]>(),
            any::<u64>(),
            ".*",
            proptest::collection::vec(write, 0..6),
        )
            .prop_map(
                |(transaction_id, validated_head, changes_made, writes)| Commit {
                    transaction_id,
                    payload: SlotPayload {
                        validated_head,
                        changes_made,
                        writes,
                    },
                },
            );

        (proptest::collection::vec(commit, 0..6), any_advert())
            .prop_map(|(commits, leader)| Envelope { commits, leader })
    }

    proptest! {
        #[test]
        fn envelopes_roundtrip(envelope in any_envelope()) {
            let decoded = Envelope::decode(&envelope.encode()).unwrap();
            prop_assert_eq!(decoded, envelope);
        }

        // Decode is total: arbitrary bytes decode as some envelope or fail
        // as `Corruption`, never panic.
        #[test]
        fn decode_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..96),
        ) {
            let _ = Envelope::decode(&bytes);
        }

        // Framed garbage is the same obligation one layer in: past the
        // header, the payload is not a valid envelope.
        #[test]
        fn decode_framed_garbage_never_panics(
            payload in proptest::collection::vec(any::<u8>(), 0..96),
        ) {
            let _ = Envelope::decode(&frame::frame(&payload));
        }
    }
}
