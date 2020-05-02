use std::collections::HashMap;

mod differential;
mod input;
mod stable_marriage;
mod v0;
mod v1;

use input::{ProposerInput, ResponderInput};
use stable_marriage::{Suited, Suitor};

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

    stable_marriage::generate_match(&mut suitors, &mut suiteds);
    println!("hello world!");

    let suitors = vec![
        Suitor::new(0, vec![3, 5, 4, 2, 1, 0]),
        Suitor::new(1, vec![2, 3, 1, 0, 4, 5]),
        Suitor::new(2, vec![5, 2, 1, 0, 3, 4]),
        Suitor::new(3, vec![0, 1, 2, 3, 4, 5]),
        Suitor::new(4, vec![4, 5, 1, 2, 0, 3]),
        Suitor::new(5, vec![0, 1, 2, 3, 4, 5]),
    ];

    let suiteds = vec![
        Suited::new(0, vec![3, 5, 4, 2, 1, 0]),
        Suited::new(1, vec![2, 3, 1, 0, 4, 5]),
        Suited::new(2, vec![5, 2, 1, 0, 3, 4]),
        Suited::new(3, vec![0, 1, 2, 3, 4, 5]),
        Suited::new(4, vec![4, 5, 1, 2, 0, 3]),
        Suited::new(5, vec![0, 1, 2, 3, 4, 5]),
    ];

    differential::generate_match(suitors, suiteds);

    let proposers = vec![
        ProposerInput::new(0, vec![3, 5, 4, 2, 1, 0]),
        ProposerInput::new(1, vec![2, 3, 1, 0, 4, 5]),
        ProposerInput::new(2, vec![5, 2, 1, 0, 3, 4]),
        ProposerInput::new(3, vec![0, 1, 2, 3, 4, 5]),
        ProposerInput::new(4, vec![4, 5, 1, 2, 0, 3]),
        ProposerInput::new(5, vec![0, 1, 2, 3, 4, 5]),
    ];

    let responders = vec![
        ResponderInput::new(0, vec![3, 5, 4, 2, 1, 0]),
        ResponderInput::new(1, vec![2, 3, 1, 0, 4, 5]),
        ResponderInput::new(2, vec![5, 2, 1, 0, 3, 4]),
        ResponderInput::new(3, vec![0, 1, 2, 3, 4, 5]),
        ResponderInput::new(4, vec![4, 5, 1, 2, 0, 3]),
        ResponderInput::new(5, vec![0, 1, 2, 3, 4, 5]),
    ];

    let matching: HashMap<u32, u32> = v0::stable_matching(&proposers, &responders).unwrap();
    let valid: bool = input::validate_matching(&proposers, &responders, &matching);

    println!("Matching: {:?} valid: {}", matching, valid);

    let proposers: Vec<ProposerInput> = proposers
        .iter()
        .map(|p| {
            let mut pref = p.preferences.clone();
            pref.reverse();
            ProposerInput::new(p.id, pref)
        })
        .collect();
    let responders: Vec<ResponderInput> = responders
        .iter()
        .map(|r| {
            let mut pref = r.preferences.clone();
            pref.reverse();
            ResponderInput::new(r.id, pref)
        })
        .collect();
    let matching: HashMap<u32, u32> = v0::stable_matching(&proposers, &responders).unwrap();
    let valid: bool = input::validate_matching(&proposers, &responders, &matching);
    println!("Matching: {:?} valid: {}", matching, valid);
}
