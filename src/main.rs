extern crate differential_dataflow;
extern crate timely;

use std::collections::btree_set::IntoIter;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Join, Reduce};

type SuitedId = u32;
type SuitorId = u32;

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Suitor {
    id: SuitorId,
    preference_set: BTreeMap<SuitedId, usize>,
    rejections: BTreeSet<SuitedId>,
}

impl Suitor {
    fn new(id: u32, preference_list: Vec<u32>) -> Self {
        Suitor {
            id,
            preference_set: BTreeMap::from_iter(
                preference_list.iter().enumerate().map(|(i, s)| (*s, i)),
            ),
            rejections: BTreeSet::new(),
        }
    }

    fn get_current_preference(&self) -> Option<SuitedId> {
        let preference = self
            .preference_set
            .iter()
            .filter(|(s, _)| !self.rejections.contains(s))
            .min_by_key(|(_, p)| *p);

        if let Some((suited, _)) = preference {
            Some(*suited)
        } else {
            None
        }
    }

    fn handle_rejection(&mut self, suited: SuitedId) {
        self.rejections.insert(suited);
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
    id: u32,
    preference_set: BTreeMap<u32, usize>,
    current_suitors: BTreeSet<u32>,
    current_accept: Option<u32>,
}

impl Suited {
    fn new(id: u32, preference_list: Vec<u32>) -> Self {
        let mut suited: Suited = Default::default();
        suited.id = id;
        suited.preference_set =
            BTreeMap::from_iter(preference_list.iter().enumerate().map(|(i, s)| (*s, i)));

        suited
    }

    fn add_suitor(&mut self, suitor: u32) {
        self.current_suitors.insert(suitor);
    }

    fn reject(&mut self) -> Option<IntoIter<u32>> {
        let preference = self
            .preference_set
            .iter()
            .filter(|(s, _)| self.current_suitors.contains(s))
            .min_by_key(|(_, p)| *p);
        let mut rejections = std::mem::replace(&mut self.current_suitors, BTreeSet::new());

        if let Some((suitor, _)) = preference {
            self.current_accept = Some(*suitor);
            rejections.remove(suitor);
            self.current_suitors.insert(*suitor);
        } else {
            self.current_accept = None;
        }

        return Some(rejections.into_iter());
    }

    fn prefers_more(&self, proposed: u32) -> bool {
        let assigned_preference = if let Some(assigned) = self.current_accept {
            self.preference_set.get(&assigned)
        } else {
            None
        };

        let proposed_preference = self.preference_set.get(&proposed);

        match (assigned_preference, proposed_preference) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(assigned), Some(proposed)) => proposed < assigned,
        }
    }
}

fn stable_marriages(suitors: &mut [Suitor], suiteds: &mut [Suited]) {
    let mut unassigned: HashSet<_> = HashSet::from_iter(suitors.iter().map(|s| s.id));
    let mut suiteds: HashMap<_, _> = HashMap::from_iter(suiteds.iter_mut().map(|s| (s.id, s)));
    let mut suitors: HashMap<_, _> = HashMap::from_iter(suitors.iter_mut().map(|s| (s.id, s)));

    while !unassigned.is_empty() {
        for suitor_id in unassigned.iter() {
            let suitor = suitors.get_mut(&suitor_id);

            if suitor.is_none() {
                continue;
            }

            let suitor = suitor.expect("suitor known to exist");

            let preference = suitor.get_current_preference();

            if preference.is_none() {
                continue;
            }

            let to_propose = suiteds.get_mut(&preference.expect("preference known to exist"));

            if let Some(to_propose) = to_propose {
                to_propose.add_suitor(suitor.id);
            }
        }

        unassigned = HashSet::new();

        for (suited_id, suited) in suiteds.iter_mut() {
            if let Some(rejections) = suited.reject() {
                for r in rejections {
                    let suitor = suitors.get_mut(&r);
                    if let Some(suitor) = suitor {
                        suitor.handle_rejection(*suited_id);
                        unassigned.insert(r);
                    }
                }
            }
        }
    }

    println!("{:?}", suiteds);

    for (suited_id, suited) in suiteds.iter() {
        println!("Suitor {:?} Suited {}", suited.current_accept, suited_id);
    }

    verify_marriage(&suitors, &suiteds);
}

fn verify_marriage(suitors: &HashMap<u32, &mut Suitor>, suiteds: &HashMap<u32, &mut Suited>) {
    let reverse: HashMap<_, _> = HashMap::from_iter(
        suiteds
            .iter()
            .filter(|(_, s)| s.current_accept.is_some())
            .map(|(id, s)| (s.current_accept.unwrap(), id)),
    );
    for (suited_id, suited) in suiteds.iter() {
        for (suitor_id, suitor) in suitors.iter() {
            // the matching is unstable if suitor and suited mutually prefer each other
            // more than who they have been matched with
            let suitor_mapping = reverse.get(suitor_id);

            if suitor_mapping.is_none() {
                continue;
            }

            let suitor_mapping = suitor_mapping.expect("known to exist");

            let suitor_prefers_more = suitor.prefers_more(**suitor_mapping, *suited_id);

            let suited_prefers_more = suited.prefers_more(*suitor_id);

            if suitor_prefers_more && suited_prefers_more {
                println!("Error: suitor {} and suited {} mutually prefer each other over their matchings", suitor_id, suited_id);
            }
        }
    }
}

fn main() {
    let mut suitors = vec![
        Suitor::new(0, vec![3, 5, 4, 2, 1, 0]),
        Suitor::new(1, vec![2, 3, 1, 0, 4, 5]),
        Suitor::new(2, vec![5, 2, 1, 0, 3, 4]),
        Suitor::new(3, vec![0, 1, 2, 3, 4, 5]),
        Suitor::new(4, vec![4, 5, 1, 2, 0, 3]),
        Suitor::new(5, vec![0, 1, 2, 3, 4, 5]),
    ];

    let mut suiteds = vec![
        Suited::new(0, vec![3, 5, 4, 2, 1, 0]),
        Suited::new(1, vec![2, 3, 1, 0, 4, 5]),
        Suited::new(2, vec![5, 2, 1, 0, 3, 4]),
        Suited::new(3, vec![0, 1, 2, 3, 4, 5]),
        Suited::new(4, vec![4, 5, 1, 2, 0, 3]),
        Suited::new(5, vec![0, 1, 2, 3, 4, 5]),
    ];

    stable_marriages(&mut suitors, &mut suiteds);
    println!("hello world!");
}
