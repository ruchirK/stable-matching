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
}

impl Proposer {
    fn new(id: ProposerId, preferences: &[ResponderId]) -> Self {
        Proposer {
            id,
            preferences: preferences
                .iter()
                .enumerate()
                .map(|(i, responder)| (*responder, i))
                .collect(),
        }
    }

    fn get_proposal(&self, rejections: &BTreeSet<ProposerId>) -> Result<ResponderId> {
        // Get the most preferred Responder that has not already rejected
        // Note that even though we are using a "filter", we are
        // computing (preferences - rejections) or an antijoin
        let proposal = self
            .preferences
            .iter()
            .filter(|(r, _)| !rejections.contains(r))
            .max_by_key(|(_, pref)| *pref);

        if let Some((responder, _)) = proposal {
            Ok(*responder)
        } else {
            bail!(
                "proposer {} has no available responders to propose to",
                self.id
            );
        }
    }
}

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Responder {
    id: ResponderId,
    // Map from ProposerId -> preference
    preferences: BTreeMap<ProposerId, usize>,
}

impl Responder {
    fn new(id: u32, preferences: &[ProposerId]) -> Self {
        Responder {
            id,
            preferences: preferences
                .iter()
                .enumerate()
                .map(|(i, proposer)| (*proposer, i))
                .collect(),
        }
    }

    fn get_response(&self, proposals: &BTreeSet<ProposerId>) -> Option<u32> {
        // Accept the ProposerId from this round of proposals that was preferred the most
        self.preferences
            .iter()
            .filter(|(p, _)| proposals.contains(p))
            .max_by_key(|(_, pref)| *pref)
            .map(|(p, _)| *p)
    }
}

pub fn stable_matching(
    proposers_input: &[ProposerInput],
    responders_input: &[ResponderInput],
) -> Result<HashMap<ProposerId, ResponderId>> {
    let proposers: HashMap<_, _> = proposers_input
        .iter()
        .map(|p| (p.id, Proposer::new(p.id, &p.preferences)))
        .collect();
    let responders: HashMap<_, _> = responders_input
        .iter()
        .map(|r| (r.id, Responder::new(r.id, &r.preferences)))
        .collect();
    let mut rejections: HashMap<_, _> = proposers_input
        .iter()
        .map(|p| (p.id, BTreeSet::new()))
        .collect();

    loop {
        // Gather a proposal from every Proposer
        let initial_proposals: HashMap<ProposerId, ResponderId> = proposers
            .iter()
            // This map is effectively a join with rejections
            .map(|(p_id, p)| (p_id, p, &rejections[p_id]))
            .map(|(p_id, p, rejections)| {
                (
                    *p_id,
                    p.get_proposal(rejections)
                        .expect("every proposer should have a proposal"),
                )
            })
            .collect();

        // Convert the proposals to become a multimap (represented by a HashMap
        // where the value is a nested BTreeMap from ResponderId -> set(ProposerIds)
        let mut proposals: HashMap<ResponderId, BTreeSet<ProposerId>> = responders
            .iter()
            .map(|(r, _)| (*r, BTreeSet::new()))
            .collect();
        for (p, r) in initial_proposals.iter() {
            proposals
                .get_mut(r)
                .expect("known to have all responders in proposals")
                .insert(*p);
        }

        // Get acceptances from all of the Responders who have received a proposal
        let initial_acceptances: HashMap<ResponderId, Option<ProposerId>> = responders
            .iter()
            // This is effectively a join with proposals
            .map(|(r_id, r)| (r_id, r, &proposals[r_id]))
            .map(|(r_id, r, proposals)| (*r_id, r.get_response(proposals)))
            .collect();

        // Reshape the mapping to be in the right format
        let mut matching: HashMap<ProposerId, ResponderId> = HashMap::new();

        for (r, p) in initial_acceptances.iter() {
            if let Some(p) = p {
                matching.insert(*p, *r);
            }
        }

        // If the matching looks complete - lets exit
        // We won't need this bit in dataflow code
        if matching.len() == proposers.len() {
            return Ok(matching);
        }

        // Update the set of rejections to include every proposal
        // from this round that wasn't accepted
        for (p, r) in initial_proposals
            .iter()
            .filter(|(p, _)| !matching.contains_key(p))
        {
            rejections
                .get_mut(p)
                .expect("known to have all proposers in rejections")
                .insert(*r);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic_v2_test() {
        crate::input::basic_test(super::stable_matching);
    }
}
