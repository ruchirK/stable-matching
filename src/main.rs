mod differential;
mod stable_marriage;

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
}
