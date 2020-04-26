use std::collections::btree_set::IntoIter;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;

use anyhow::Result;

use crate::input::{ProposerId, ProposerInput, ResponderId, ResponderInput};

#[derive(Debug, Default, Eq, Hash, PartialEq)]
struct Proposer<'a> {
    id: ProposerId,
    // List of Responders ordered by ascending preference
    preferences: &'a [ResponderId],
    preferences_index: usize,
}

impl<'a> Proposer<'a> {
    fn new(id: ProposerId, preferences: &'a [ResponderId]) -> Self {
        Proposer {
            id,
            preferences,
            preferences_index: preferences.len() - 1,
            // preferences_index: 0,
        }
    }

    fn get_preference(&self) -> ResponderId {
        self.preferences[self.preferences_index]
    }

    fn add_rejection(&mut self) {
        if self.preferences_index > 0 {
            self.preferences_index -= 1;
        } else {
            panic!("proposer {} received too many rejections", self.id);
        }
    }
}

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Responder<'a> {
    id: ResponderId,
    // List of Proposers ordered by ascending preference
    preferences: &'a [ProposerId],
    // Mapping from ProposerId -> preference
    // Relies strongly on preferences and ProposerIds both being in the domain
    // [0, n) where n is the number of Proposers
    preferences_by_proposer: Vec<usize>,
    proposals: BTreeSet<ProposerId>,
    accepted: Option<ProposerId>,
}

impl<'a> Responder<'a> {
    fn new(id: u32, preferences: &'a [ProposerId]) -> Self {
        let mut preferences_by_proposer: Vec<usize> = vec![0; preferences.len()];

        for (index, p) in preferences.iter().enumerate() {
            preferences_by_proposer[*p as usize] = index;
        }

        Responder {
            id,
            preferences,
            preferences_by_proposer,
            proposals: BTreeSet::new(),
            accepted: None,
        }
    }

    fn add_proposal(&mut self, proposer: ProposerId) {
        self.proposals.insert(proposer);
    }

    fn reject(&mut self) -> Option<IntoIter<u32>> {
        if self.proposals.is_empty() {
            return None;
        }

        let mut accept = *self
            .proposals
            .iter()
            .nth(0)
            .expect("proposals known to have elements");

        if accept >= self.preferences.len() as ProposerId {
            panic!(
                "Received proposal {} that was outside of expected range {}",
                accept,
                self.preferences.len()
            );
        }

        let mut accept_preference = self.preferences_by_proposer[accept as usize];

        for p in self.proposals.iter().skip(1) {
            if *p >= self.preferences.len() as ProposerId {
                // We've received a proposer that we did not anticipate. Lets get out
                panic!(
                    "Received proposal {} that was outside of expected range {}",
                    *p,
                    self.preferences.len()
                );
            }

            let preference = self.preferences_by_proposer[*p as usize];
            if preference > accept_preference {
                accept = *p;
                accept_preference = preference;
            }
        }

        self.accepted = Some(accept);

        // Reject every proposal we didn't accept
        let mut rejections = std::mem::replace(&mut self.proposals, BTreeSet::new());
        rejections.remove(&accept);

        // We want to preserve the invariant that proposals always contains the last proposal
        // we tentatively accepted + future proposals
        self.proposals.insert(accept);

        return Some(rejections.into_iter());
    }
}

pub fn stable_matching(
    proposers_input: &[ProposerInput],
    responders_input: &[ResponderInput],
) -> Result<HashMap<ProposerId, ResponderId>> {
    let mut proposers: Vec<_> = Vec::from_iter(
        proposers_input
            .iter()
            .map(|p| Proposer::new(p.id, &p.preferences)),
    );
    let mut responders: Vec<_> = Vec::from_iter(
        responders_input
            .iter()
            .map(|r| Responder::new(r.id, &r.preferences)),
    );
    let mut unassigned: HashSet<_> = HashSet::from_iter(proposers_input.iter().map(|p| p.id));

    while !unassigned.is_empty() {
        // All unassigned Proposers propose to their highest ranked Responder
        // that has not already rejected them
        for p in unassigned.iter() {
            let proposer = &proposers[*p as usize];
            let preference = proposer.get_preference();
            let to_propose = &mut responders[preference as usize];
            to_propose.add_proposal(*p);
        }

        unassigned = HashSet::new();

        // All Responders check if they have to reject any Proposers
        // Any Proposers that have been rejected are therefore unassigned
        for r in responders.iter_mut() {
            if let Some(rejections) = r.reject() {
                unassigned.extend(rejections);
            }
        }

        // All rejected Proposers need to update who they can propose to next time
        for p in unassigned.iter() {
            let proposer = &mut proposers[*p as usize];
            proposer.add_rejection();
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
