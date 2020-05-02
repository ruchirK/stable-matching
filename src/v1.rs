use std::collections::btree_set::IntoIter;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;

use anyhow::{bail, Result};

use crate::input::{ProposerId, ProposerInput, ResponderId, ResponderInput};

#[derive(Debug, Default, Eq, Hash, PartialEq)]
struct Proposer {
    id: ProposerId,
    // Map from ResponderId -> preference
    preferences: BTreeMap<ResponderId, usize>,
    rejections: BTreeSet<ResponderId>,
}

impl Proposer {
    fn new(id: ProposerId, preferences: &[ResponderId]) -> Result<Self> {
        Ok(Proposer {
            id,
            preferences: preferences
                .iter()
                .enumerate()
                .map(|(i, responder)| (*responder, i))
                .collect(),
            rejections: BTreeSet::new(),
        })
    }

    fn get_preference(&self) -> Result<ResponderId> {
        // Get the most preferred Responder that has not already rejected
        // Note that even though we are using a "filter", we are
        // computing preferences - rejections or antijoins
        let preference = self
            .preferences
            .iter()
            .filter(|(r, _)| !self.rejections.contains(r))
            .max_by_key(|(_, pref)| *pref);

        if let Some((responder, _)) = preference {
            Ok(*responder)
        } else {
            bail!(
                "proposer {} has no available responders to propose to",
                self.id
            );
        }
    }

    fn add_rejection(&mut self, responder: ResponderId) {
        self.rejections.insert(responder);
    }
}

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Responder {
    id: ResponderId,
    // Map from ProposerId -> preference
    preferences: BTreeMap<ProposerId, usize>,
    proposals: BTreeSet<ProposerId>,
    accepted: Option<ProposerId>,
}

impl Responder {
    fn new(id: u32, preferences: &[ProposerId]) -> Result<Self> {
        Ok(Responder {
            id,
            preferences: preferences
                .iter()
                .enumerate()
                .map(|(i, proposer)| (*proposer, i))
                .collect(),
            proposals: BTreeSet::new(),
            accepted: None,
        })
    }

    fn add_proposal(&mut self, proposer: ProposerId) {
        self.proposals.insert(proposer);
    }

    fn reject(&mut self) -> Option<IntoIter<u32>> {
        // Accept the ProposerId from this round of proposals that was preferred the most
        self.accepted = self
            .preferences
            .iter()
            .filter(|(p, _)| self.proposals.contains(p))
            .max_by_key(|(_, pref)| *pref)
            .map(|(p, _)| *p);

        // Reject every proposal that wasn't accepted
        let mut rejections = std::mem::replace(&mut self.proposals, BTreeSet::new());
        if let Some(accept) = self.accepted {
            rejections.remove(&accept);

            // We want to preserve the invariant that proposals always contains the last proposal
            // we tentatively accepted + future proposals
            self.proposals.insert(accept);
        }

        return Some(rejections.into_iter());
    }
}

pub fn stable_matching(
    proposers_input: &[ProposerInput],
    responders_input: &[ResponderInput],
) -> Result<HashMap<ProposerId, ResponderId>> {
    let mut proposers: Vec<_> = proposers_input
        .iter()
        .map(|p| Proposer::new(p.id, &p.preferences))
        .collect::<Result<_>>()?;
    let mut responders: Vec<_> = responders_input
        .iter()
        .map(|r| Responder::new(r.id, &r.preferences))
        .collect::<Result<_>>()?;
    let mut unassigned: HashSet<_> = HashSet::from_iter(proposers_input.iter().map(|p| p.id));

    while !unassigned.is_empty() {
        // All unassigned Proposers propose to their highest ranked Responder
        // that has not already rejected them
        for p in unassigned.iter() {
            let proposer = &proposers[*p as usize];
            let preference = proposer.get_preference()?;
            let to_propose = &mut responders[preference as usize];
            to_propose.add_proposal(*p);
        }

        unassigned = HashSet::new();

        // All Responders check if they have to reject any Proposers
        // Any Proposers that have been rejected are therefore unassigned
        for r in responders.iter_mut() {
            if let Some(rejections) = r.reject() {
                for rejected_proposer in rejections {
                    let proposer = &mut proposers[rejected_proposer as usize];
                    proposer.add_rejection(r.id);
                    unassigned.insert(rejected_proposer);
                }
            }
        }
    }

    // Return a mapping from ProposerId : ResponderId
    Ok(HashMap::from_iter(responders.iter().map(|r| {
        (
            r.accepted
                .expect("every responder should be matched with a proposer"),
            r.id,
        )
    })))
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic_v0_test() {
        let mut rng = rand::thread_rng();
        for n in 1..100 {
            let (proposers, responders) = crate::input::random_input(n, &mut rng);

            let matching = super::stable_matching(&proposers, &responders).unwrap();

            assert_eq!(
                crate::input::validate_matching(&proposers, &responders, &matching),
                true
            );
        }
    }
}
