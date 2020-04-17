use std::collections::btree_set::IntoIter;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;

pub type SuitedId = u32;
pub type SuitorId = u32;

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Suitor {
    pub id: SuitorId,
    pub preference_set: BTreeMap<SuitedId, usize>,
}

impl Suitor {
    pub fn new(id: u32, preference_list: Vec<u32>) -> Self {
        Suitor {
            id,
            preference_set: BTreeMap::from_iter(
                preference_list.iter().enumerate().map(|(i, s)| (*s, i)),
            ),
        }
    }

    fn get_current_preference(&self, rejections: &BTreeSet<SuitedId>) -> Option<SuitedId> {
        let preference = self
            .preference_set
            .iter()
            .filter(|(s, _)| !rejections.contains(s))
            .min_by_key(|(_, p)| *p);

        if let Some((suited, _)) = preference {
            Some(*suited)
        } else {
            None
        }
    }

    fn prefers_more(&self, assigned: u32, proposed: u32) -> bool {
        let assigned_preference = self.preference_set.get(&assigned);
        let proposed_preference = self.preference_set.get(&proposed);

        match (assigned_preference, proposed_preference) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(assigned), Some(proposed)) => proposed < assigned,
        }
    }
}

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Suited {
    pub id: u32,
    pub preference_set: BTreeMap<u32, usize>,
}

impl Suited {
    pub fn new(id: u32, preference_list: Vec<u32>) -> Self {
        let mut suited: Suited = Default::default();
        suited.id = id;
        suited.preference_set =
            BTreeMap::from_iter(preference_list.iter().enumerate().map(|(i, s)| (*s, i)));

        suited
    }

    fn get_current_accept(&self, proposals: &BTreeSet<SuitorId>) -> Option<u32> {
        let preference = self
            .preference_set
            .iter()
            .filter(|(s, _)| proposals.contains(s))
            .min_by_key(|(_, p)| *p);

        if let Some((suitor, _)) = preference {
            Some(*suitor)
        } else {
            None
        }
    }

    fn prefers_more(&self, assigned: u32, proposed: u32) -> bool {
        let assigned_preference = self.preference_set.get(&assigned);
        let proposed_preference = self.preference_set.get(&proposed);

        match (assigned_preference, proposed_preference) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(assigned), Some(proposed)) => proposed < assigned,
        }
    }
}

pub fn generate_match(suitors: &[Suitor], suiteds: &[Suited]) {
    let mut unassigned: HashSet<_> = HashSet::from_iter(suitors.iter().map(|s| s.id));
    let mut rejections: HashMap<_, _> =
        HashMap::from_iter(suitors.iter().map(|s| (s.id, BTreeSet::new())));
    let mut matching: HashMap<SuitorId, SuitedId> = HashMap::new();
    let suiteds: HashMap<_, _> = HashMap::from_iter(suiteds.iter().map(|s| (s.id, s)));
    let suitors: HashMap<_, _> = HashMap::from_iter(suitors.iter().map(|s| (s.id, s)));

    while !unassigned.is_empty() {
        let mut proposals: HashMap<_, _> =
            HashMap::from_iter(suiteds.iter().map(|(id, _)| (id, BTreeSet::new())));

        for suitor_id in unassigned.iter() {
            let suitor = suitors.get(&suitor_id);

            if suitor.is_none() {
                continue;
            }

            let rejection = rejections.get_mut(&suitor_id);

            if rejection.is_none() {
                continue;
            }

            let suitor = suitor.expect("suitor known to exist");
            let rejection = rejection.expect("rejection known to exist");

            let preference = suitor.get_current_preference(&rejection);

            if preference.is_none() {
                continue;
            }

            let to_propose = proposals.get_mut(&preference.expect("preference known to exist"));

            if let Some(to_propose) = to_propose {
                to_propose.insert(suitor.id);
            }
        }

        unassigned = HashSet::new();

        for (suited_id, suited) in suiteds.iter() {
            let proposal = proposals.get_mut(&suited_id);

            if proposal.is_none() {
                continue;
            }

            let proposal = proposal.expect("proposal known to exist");

            let suited_preference = suited.get_current_accept(&proposal);

            if let Some(suited_preference) = suited_preference {
                proposal.remove(&suited_preference);
                // this has replace semantics
                matching.insert(suited_preference, *suited_id);
            }

            for r in proposal.iter() {
                let rejection = rejections.get_mut(&r);
                if let Some(rejection) = rejection {
                    rejection.insert(*suited_id);
                    unassigned.insert(*r);
                }
            }
        }
    }

    println!("{:?}", matching);

    verify_match(&suitors, &suiteds, &matching);
}

fn verify_match(
    suitors: &HashMap<u32, &Suitor>,
    suiteds: &HashMap<u32, &Suited>,
    matching: &HashMap<SuitorId, SuitedId>,
) {
    let reverse: HashMap<_, _> =
        HashMap::from_iter(matching.iter().map(|(suitor, suited)| (suited, suitor)));
    for (suited_id, suited) in suiteds.iter() {
        for (suitor_id, suitor) in suitors.iter() {
            // the matching is unstable if suitor and suited mutually prefer each other
            // more than who they have been matched with

            let suitor_mapping = matching.get(suitor_id);
            let suited_mapping = reverse.get(suited_id);

            if suitor_mapping.is_none() || suited_mapping.is_none() {
                continue;
            }

            let suitor_mapping = suitor_mapping.expect("known to exist");
            let suited_mapping = suited_mapping.expect("known to exist");

            let suitor_prefers_more = suitor.prefers_more(*suitor_mapping, *suited_id);

            let suited_prefers_more = suited.prefers_more(**suited_mapping, *suitor_id);

            if suitor_prefers_more && suited_prefers_more {
                println!("Error: suitor {} and suited {} mutually prefer each other over their matchings", suitor_id, suited_id);
            }
        }
    }
}
