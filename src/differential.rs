extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Reduce};
use differential_dataflow::Collection;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;

use crate::stable_marriage::{Suited, Suitor};

pub fn generate_match(suitors: Vec<Suitor>, suiteds: Vec<Suited>) {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = ProbeHandle::new();

        let (mut suitors_collection, mut suiteds_collection) = worker.dataflow(|scope| {
            let (suitors_input, suitors) = scope.new_collection();
            let (suiteds_input, suiteds) = scope.new_collection();

            let mut result = generate_match_dataflow(&suitors, &suiteds);

            result
                .inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);

            (suitors_input, suiteds_input)
        });

        for suitor in &suitors {
            for (suited, preference) in suitor.preference_set.iter() {
                suitors_collection.insert((suitor.id, *suited, *preference));
            }
        }

        for suited in &suiteds {
            for (suitor, preference) in suited.preference_set.iter() {
                suiteds_collection.insert((suited.id, *suitor, *preference));
            }
        }

        suitors_collection.advance_to(1 as u32);
        suiteds_collection.advance_to(1 as u32);

        suiteds_collection.flush();
        suitors_collection.flush();

        worker.step_while(|| {
            probe.less_than(suitors_collection.time()) || probe.less_than(suiteds_collection.time())
        });

        println!("Stable!");
    })
    .expect("completed without errors");
}

fn generate_match_dataflow<G: Scope>(
    suitors: &Collection<G, (u32, u32, usize)>,
    suiteds: &Collection<G, (u32, u32, usize)>,
) -> Collection<G, (u32, u32)>
where
    G::Timestamp: Lattice + Ord,
{
    // initialize all suitors to initial propose to their first choicee
    let mapping = suitors
        .map(|(suitor, suited, preference)| (suitor, (suited, preference)))
        .reduce(|_key, input, output| {
            let mut min_index = 0;

            for i in 1..input.len() {
                if (input[i].0).1 < (input[min_index].0).1 {
                    min_index = i;
                }
            }

            output.push(((input[min_index].0).0, 1));
        }); //.map(|(suitor, (suited, preference))| (suitor, suited));

    let unassigned = suitors
        .map(|(suitor, suited, preference)| (suitor, (suited, preference)))
        .reduce(|_key, _input, output| {
            output.push(((), 1));
        })
        .map(|(suitor, ())| (suitor))
        .inspect(|x| println!("{:?}", x));

    mapping
}
