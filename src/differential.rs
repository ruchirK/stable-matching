extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Consolidate, Iterate, Join, Reduce};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::Collection;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::order::Product;

use crate::stable_marriage::{Suited, Suitor};

pub fn generate_match(suitors: Vec<Suitor>, suiteds: Vec<Suited>) {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = ProbeHandle::new();

        let (mut suitors_collection, mut suiteds_collection) = worker.dataflow(|scope| {
            let (suitors_input, suitors) = scope.new_collection();
            let (suiteds_input, suiteds) = scope.new_collection();

            let mut result = generate_match_dataflow(scope, &suitors, &suiteds);

            result
                .inspect(|x| println!("result: {:?}", x))
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

fn generate_match_dataflow<G: Scope<Timestamp=u32>>(
    _scope: &mut G,
    suitors: &Collection<G, (u32, u32, usize)>,
    suiteds: &Collection<G, (u32, u32, usize)>,
) -> Collection<G, (u32, u32)>
where
    G::Timestamp: Lattice + Ord,
{
    let rejections = suitors
        .map(|(suitor, suited, _)| (suitor, suited))
        .filter(|_| false);

    rejections.inner.scope().scoped::<Product<u32, u32>, _, _,>("Test", |nested| {
            let summary = Product::new(Default::default(), 1);
            let rejections_inner  = Variable::new_from(rejections.enter(nested), summary);

            let suitors = suitors.enter(&rejections_inner.scope());
            let suiteds = suiteds.enter(&rejections_inner.scope());

            rejections_inner.inspect(|x| println!("rejections (nested): (suitor, suited): {:?}", x));

            let proposals = suitors
                .map(|(suitor, suited, preference)| ((suitor, suited), preference))
                .antijoin(&rejections_inner)
                .map(|((suitor, suited), preference)| (suitor, (preference, suited)))
                .reduce(|_suitor, input, output| {
                    let mut min_index = 0;

                    for i in 1..input.len() {
                        if (input[i].0).0 < (input[min_index].0).0 {
                            min_index = i;
                        }
                    }

                    output.push((*input[min_index].0, 1));
                })
                .map(|(suitor, (preference, suited))| ((suited, suitor), ()))
                .inspect(|x| println!("proposal (nested): (suited, suitor)  {:?}", x));

            let acceptances_inner = suiteds
                .map(|(suited, suitor, preference)| ((suited, suitor), preference))
                .join(&proposals)
                .map(|((suited, suitor), preference)| (suited, (preference, suitor)))
                .reduce(|_suited, input, output| {
                    let mut min_index = 0;

                    for i in 1..input.len() {
                        if (input[i].0).0 < (input[min_index].0).0 {
                            min_index = i;
                        }
                    }

                    output.push((*input[min_index].0, 1));
                })
                .map(|(suited, (preference, suitor))| (suited, suitor))
                .inspect(|x| println!("acceptances (nested): (suited, suitor) {:?}", x));

            let rejected = proposals
                .antijoin(&acceptances_inner)
                .map(|((suited, suitor), ())| (suitor, suited));
            let final_rejections = rejections_inner.concat(&rejected).consolidate();
            rejections_inner.set(&final_rejections);
            final_rejections.leave()
        });
    let proposals = suitors
        .map(|(suitor, suited, preference)| ((suitor, suited), preference))
        .antijoin(&rejections)
        .map(|((suitor, suited), preference)| (suitor, (preference, suited)))
        .reduce(|_suitor, input, output| {
            let mut min_index = 0;

            for i in 1..input.len() {
                if (input[i].0).0 < (input[min_index].0).0 {
                    min_index = i;
                }
            }

            output.push((*input[min_index].0, 1));
        })
        .map(|(suitor, (preference, suited))| ((suited, suitor), ()))
        .inspect(|x| println!("proposal: (suited, suitor)  {:?}", x));

    let acceptances = suiteds
        .map(|(suited, suitor, preference)| ((suited, suitor), preference))
        .join(&proposals)
        .map(|((suited, suitor), preference)| (suited, (preference, suitor)))
        .reduce(|_suited, input, output| {
            let mut min_index = 0;

            for i in 1..input.len() {
                if (input[i].0).0 < (input[min_index].0).0 {
                    min_index = i;
                }
            }

            output.push((*input[min_index].0, 1));
        })
        .map(|(suited, (preference, suitor))| (suitor, suited))
        .inspect(|x| println!("acceptances (suitor, suited) {:?}", x));

    acceptances
}
