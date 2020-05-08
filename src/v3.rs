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

            let invalid = validate_matching(&proposers, &responders, &matching)
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
    // Idiom to get an empty collection
    // TODO can we do something better?
    let rejections = proposers
        .map(|(proposer, responder, _)| (proposer, responder))
        // This filter makes it so none of the records become part of the
        // rejections collection
        .filter(|_| false);

    rejections
        .inner
        .scope()
        .scoped::<Product<G::Timestamp, u32>, _, _>("ComputeStableMatching", |nested| {
            let summary = Product::new(Default::default(), 1);
            let rejections_inner = Variable::new_from(rejections.enter(nested), summary);

            // Bring the responder and proposer collections into the iterative computation
            let proposers = proposers.enter(&rejections_inner.scope());
            let responders = responders.enter(&rejections_inner.scope());

            //rejections_inner
            //    .inspect(|x| println!("rejections (proposer, reponder): {:?}", x));

            let proposals = proposers
                .map(|(proposer, responder, preference)| ((proposer, responder), preference))
                .antijoin(&rejections_inner)
                .map(|((proposer, responder), preference)| (proposer, (preference, responder)))
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
                // We need to re-key by (responder, preference) to join with the data we got from the
                // responders, and we need to specify an empty value because joins take two collections of
                // (k, v1) and (k, v2) and give you (k, (v1, v2)) for matching keys
                .map(|(proposer, (preference, responder))| ((responder, proposer), ()));
            //.inspect(|x| println!("proposal (nested): (suited, suitor)  {:?}", x));

            let accepted = responders
                .map(|(responder, proposer, preference)| ((responder, proposer), preference))
                .join(&proposals)
                .map(|((responder, proposer), preference)| (responder, (preference, proposer)))
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
                .map(|(responder, (_, proposer))| (proposer, responder))
                .consolidate();
            //.inspect(|x| println!("acceptances (nested): (suited, suitor) {:?}", x));

            let rejected = proposals
                .map(|((responder, proposer), ())| (proposer, responder))
                .concat(&accepted.negate());
            let final_rejections = rejections_inner.concat(&rejected).consolidate();
            rejections_inner.set(&final_rejections);
            accepted.leave()
        })
}

fn validate_matching<G: Scope>(
    proposers: &Collection<G, (u32, u32, usize)>,
    responders: &Collection<G, (u32, u32, usize)>,
    matching: &Collection<G, (u32, u32)>,
) -> Collection<G, (u32, u32)>
where
    G::Timestamp: Lattice + Ord,
{
    // Re-add proposer and responder preferences to the (proposer, responder) assignments
    let matching = matching
        .map(|(proposer, responder)| ((proposer, responder), ()))
        .join(&proposers.map(|(proposer, responder, pref)| ((proposer, responder), pref)))
        .join(&responders.map(|(responder, proposer, pref)| ((proposer, responder), pref)))
        .map(
            |((proposer, responder), ((_, proposer_pref), responder_pref))| {
                (proposer, proposer_pref, responder, responder_pref)
            },
        );

    // Get the list of (proposer, alternate_responder) pairs where the proposer preferred alternate_responder more than the responder it was assigned to
    let proposer_preferred_more = matching
        .map(|(proposer, proposer_pref, responder, responder_pref)| {
            (proposer, (proposer_pref, responder, responder_pref))
        })
        .join(&proposers.map(|(proposer, responder, pref)| (proposer, (responder, pref))))
        .filter(|(_, ((assigned_pref, _, _), (_, alternate_pref)))| alternate_pref > assigned_pref)
        .map(|(proposer, ((_, _, _), (alternate_responder, _)))| {
            ((proposer, alternate_responder), ())
        });

    // Get the list of (alternate_proposer, responder) pairs where the responder preffered alternate_proposer more than the proposer it was assigned to
    let responder_preferred_more = matching
        .map(|(proposer, proposer_pref, responder, responder_pref)| {
            (responder, (responder_pref, proposer, proposer_pref))
        })
        .join(&responders.map(|(responder, proposer, pref)| (responder, (proposer, pref))))
        .filter(|(_, ((assigned_pref, _, _), (_, alternate_pref)))| alternate_pref > assigned_pref)
        .map(|(responder, ((_, _, _), (alternate_proposer, _)))| {
            ((alternate_proposer, responder), ())
        });

    // See if any (proposer, responder)'s mutually preferred each other over their assignments
    proposer_preferred_more
        .join(&responder_preferred_more)
        .map(|((proposer, responder), (_, _))| (proposer, responder))
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic_v3_test() {
        let mut rng = rand::thread_rng();
        for n in 1..100 {
            println!("starting test {}", n);
            let (proposers, responders) = crate::input::random_input(n, &mut rng);

            super::stable_matching(proposers, responders);
        }
    }
}
