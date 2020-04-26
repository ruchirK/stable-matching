use std::collections::HashMap;
use std::iter::{FromIterator, Iterator};
use std::slice::Iter;

pub type ProposerId = u32;
pub type ResponderId = u32;

#[derive(Debug)]
pub struct ProposerInput {
    pub id: ProposerId,
    pub preferences: Vec<ResponderId>,
}

#[derive(Debug)]
pub struct ResponderInput {
    pub id: ResponderId,
    pub preferences: Vec<ProposerId>,
}

impl ProposerInput {
    pub fn new(id: ProposerId, preferences: Vec<ResponderId>) -> Self {
        ProposerInput { id, preferences }
    }
}

impl ResponderInput {
    pub fn new(id: ResponderId, preferences: Vec<ProposerId>) -> Self {
        ResponderInput { id, preferences }
    }
}

pub trait MatchingInput {
    fn id(&self) -> u32;

    fn preferences(&self) -> Iter<u32>;

    fn prefers_more(&self, assigned: u32, alternative: u32) -> bool {
        let mut assigned_pref = None;
        let mut alternative_pref = None;

        for (i, x) in self.preferences().enumerate() {
            if *x == assigned {
                assigned_pref = Some(i);
            } else if *x == alternative {
                alternative_pref = Some(i);
            }
        }

        match (assigned_pref, alternative_pref) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(assigned), Some(alternative)) => alternative > assigned,
        }
    }
}

impl MatchingInput for ProposerInput {
    fn id(&self) -> u32 {
        self.id
    }

    fn preferences(&self) -> Iter<u32> {
        self.preferences.iter()
    }
}

impl MatchingInput for ResponderInput {
    fn id(&self) -> u32 {
        self.id
    }

    fn preferences(&self) -> Iter<u32> {
        self.preferences.iter()
    }
}

pub fn validate_matching(
    proposers: &[ProposerInput],
    responders: &[ResponderInput],
    matching: &HashMap<u32, u32>,
) -> bool {
    let reverse: HashMap<_, _> = HashMap::from_iter(matching.iter().map(|(p, r)| (r, p)));

    for p in proposers.iter() {
        for r in responders.iter() {
            let proposer_match = matching.get(&p.id());
            let responder_match = reverse.get(&r.id());

            if proposer_match.is_none() || responder_match.is_none() {
                // This matching has to be invalid because every proposer and responder should have
                // been matched.
                return false;
            }

            let proposer_match = proposer_match.expect("proposer match known to exist");
            let responder_match = responder_match.expect("responder match known to exist");

            let proposer_prefers_more = p.prefers_more(*proposer_match, r.id());
            let responder_prefers_more = r.prefers_more(**responder_match, p.id());

            if proposer_prefers_more && responder_prefers_more {
                // Both the proposer p and the responder r mutually prefer each other over their
                // respective assignments so the matching was not stable
                return false;
            }
        }
    }

    return true;
}