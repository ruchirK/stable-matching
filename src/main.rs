extern crate differential_dataflow;
extern crate timely;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Reduce, Join};

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Suitor {
    id: u32,
    preference_list: Vec<u32>,
    preference_index: u32,
}

impl Suitor {
    fn new(id: u32, preference_list: Vec<u32>) -> Self {
        Suitor {
            id,
            preference_list,
            preference_index: 0,
        }
    }

    fn get_current_preference(&self) -> Option<u32> {
        if self.preference_index < self.preference_list.len() as u32 {
            Some(self.preference_list[self.preference_index as usize])
        } else {
            None
        }
    }

    fn increment_preference_index(&mut self) {
        self.preference_index += 1;
    }
}

#[derive(Debug, Default, Eq, Hash, PartialEq)]
pub struct Suited {
    id: u32,
    preference_list: Vec<u32>,
    current_suitors: BTreeSet<u32>,
    current_accept: Option<u32>,
}

impl Suited {
    fn new(id: u32, preference_list: Vec<u32>) -> Self {
        let mut suited: Suited = Default::default();
        suited.id = id;
        suited.preference_list = preference_list;

        suited
    }

    fn add_suitor(&mut self, suitor: u32) {
        self.current_suitors.insert(suitor);
    }

    fn reject(&mut self) -> Option<BTreeSet<u32>> {
        let mut accept_preference = u32::max_value();
        let mut accept_suitor = 0;

        if self.current_suitors.is_empty() {
            return None
        }

        for suitor in self.current_suitors.iter() {
            if self.preference_list[*suitor as usize] < accept_preference {
                accept_preference = self.preference_list[*suitor as usize];
                accept_suitor = *suitor;
            }
        }

        self.current_accept = Some(accept_suitor);
        let mut ret = std::mem::replace(&mut self.current_suitors, BTreeSet::new());
        ret.remove(&self.current_accept.expect("current suitor known to exist"));
        self.current_suitors.insert(self.current_accept.expect("current suitor known to exist"));

        return Some(ret);
    }

}

fn stable_marriages(suitors: &mut [Suitor], suiteds: &mut [Suited]) {
    let mut unassigned: HashSet<_> = HashSet::from_iter(suitors.iter());
    let suiteds: HashMap<_ , _> = HashMap::from_iter(suiteds.iter().map(|s| (s.id, s)));

    while !unassigned.is_empty() {
        for suitor in unassigned.iter() {
            let preference = suitor.get_current_preference();

            if preference.is_none() {
                continue;
            }

            let to_propose = suiteds.get(&preference.expect("preference known to exist"));

            if let Some(to_propose) = to_propose {
                to_propose.add_suitor(suitor.id);
            }
        }

        let unassigned = HashSet::new();

        for (_, suited) in suiteds.iter() {
            if let Some(rejections) = suited.reject() {
                unassigned = unassigned.union(&rejections);
            }
        }

        for suitor in unassigned.iter() {
            suitor.increment_preference_index();
        }i
    }

    println!("{:?}", suiteds);
}

fn main(){
    println!("hello world!");
}
