use std::collections::HashMap;

use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Consolidate, Join, Reduce};
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::order::Product;

use crate::input::{ProposerInput, ResponderInput};

pub fn stable_matching(proposers: Vec<ProposerInput>, responders: Vec<ResponderInput>) {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = ProbeHandle::new();

        let (mut proposers_input, mut responders_input) = worker.dataflow(|scope| {
            let (proposers_input, proposers) = scope.new_collection();
            let (responders_input, responders) = scope.new_collection();

            let matching = generate_matching(&proposers, &responders);

            let result = matching
                //.inspect(|x| println!("result: {:?}", x))
                .probe_with(&mut probe);

            let invalid = crate::v3::validate_matching(&proposers, &responders, &matching)
                .probe_with(&mut probe)
                .assert_empty();

            (proposers_input, responders_input)
        });

        for proposer in &proposers {
            for (preference, responder) in proposer.preferences.iter().enumerate() {
                proposers_input.insert((proposer.id, *responder, preference));
            }
        }

        for responder in &responders {
            for (preference, proposer) in responder.preferences.iter().enumerate() {
                responders_input.insert((responder.id, *proposer, preference));
            }
        }

        proposers_input.advance_to(1 as u32);
        responders_input.advance_to(1 as u32);

        proposers_input.flush();
        responders_input.flush();

        worker.step_while(|| {
            probe.less_than(proposers_input.time()) || probe.less_than(responders_input.time())
        });
    })
    .expect("completed without errors");
}

fn generate_matching<G: Scope>(
    proposers: &Collection<G, (u32, u32, usize)>,
    responders: &Collection<G, (u32, u32, usize)>,
) -> Collection<G, (u32, u32)>
where
    G::Timestamp: Lattice + Ord,
{
    let active = proposers
        .map(|(proposer, responder, pref)| ((proposer, responder), pref))
        .join(&responders.map(|(responder, proposer, pref)| ((proposer, responder), pref)))
        .map(|((proposer, responder), (proposer_pref, responder_pref))| {
            (proposer, proposer_pref, responder, responder_pref)
        });

    active
        .inner
        .scope()
        .scoped::<Product<G::Timestamp, u32>, _, _>("ComputeStableMatching", |nested| {
            let summary = Product::new(Default::default(), 1);
            let active_inner = Variable::new_from(active.enter(nested), summary);

            let proposals = active_inner
                .map(|(proposer, proposer_pref, responder, responder_pref)| {
                    (proposer, (proposer_pref, responder, responder_pref))
                })
                // Now grab the best proposal for each proposer
                .reduce(|_proposer, input, output| {
                    let mut max_index = 0;

                    for i in 1..input.len() {
                        if (input[i].0).0 > (input[max_index].0).0 {
                            max_index = i;
                        }
                    }

                    // Put the record corresponding to the best proposal to the output of this operator
                    // with multiplicity 1 (aka "insert")
                    output.push((*input[max_index].0, 1));
                })
                .map(|(proposer, (proposer_pref, responder, responder_pref))| {
                    (proposer, proposer_pref, responder, responder_pref)
                });
            //.inspect(|x| println!("proposal (nested): (suited, suitor)  {:?}", x));

            let accepted = proposals
                .map(|(proposer, proposer_pref, responder, responder_pref)| {
                    (responder, (responder_pref, proposer, proposer_pref))
                })
                // Now grab the best proposal each responder received
                .reduce(|_responder, input, output| {
                    let mut max_index = 0;

                    for i in 1..input.len() {
                        if (input[i].0).0 > (input[max_index].0).0 {
                            max_index = i;
                        }
                    }

                    output.push((*input[max_index].0, 1));
                })
                .map(|(responder, (responder_pref, proposer, proposer_pref))| {
                    (proposer, proposer_pref, responder, responder_pref)
                });
            //.inspect(|x| println!("acceptances (nested): (suited, suitor) {:?}", x));

            let active_final = &active_inner
                .concat(&proposals.negate())
                .concat(&accepted)
                .consolidate();
            active_inner.set(&active_final);
            accepted
                .map(|(proposer, _, responder, _)| (proposer, responder))
                .leave()
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic_v4_test() {
        let mut rng = rand::thread_rng();
        for n in 1..100 {
            println!("starting test {}", n);
            let (proposers, responders) = crate::input::random_input(n, &mut rng);

            super::stable_matching(proposers, responders);
        }
    }
}
