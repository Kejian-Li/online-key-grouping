package com.okg.actor;

import akka.actor.AbstractFSM;
import com.okg.message.RoutingTable;
import com.okg.message.Sketch;
import com.okg.message.Tuple;
import com.okg.state.SchedulerState;
import com.okg.util.SpaceSaving;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public class SchedulerActor extends AbstractFSM<SchedulerState, MessageQueue> {


    private static int N = 1000;
    private static int M = 100000;
    private int n = 0;

    private Sketch sketch;
    private SpaceSaving spaceSaving;

    private static final double epsilon = 0.05;
    private static final double theta = 0.1;


    {
        startWith(SchedulerState.HASH, null);
        sketch = new Sketch();
        spaceSaving = new SpaceSaving(epsilon, theta);   // wait a moment
    }


    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        when(SchedulerState.HASH,
                matchEvent(Tuple.class, (tuple, container) -> {
                    n++;
                    int index = hash(tuple);

                    if (n == N) {
                        n = 0;    // reuse
                        goTo(SchedulerState.COLLECT);
                    }
                }));
        when(SchedulerState.COLLECT,
                matchEvent(Tuple.class, ((tuple, container) -> {
                    n++;
                    container.add(tuple);

                    if (n == M) {
                        n = 0;
                        sendSketch();
                        refresh();
                        goTo(SchedulerState.WAIT);
                    }
                })));

        when(SchedulerState.WAIT,
                matchEvent(Tuple.class, (tuple, container) -> {
                    n++;
                    container.add(tuple);
                })
                        .event(RoutingTable.class, () -> {
                            goTo(SchedulerState.ASSIGN);
                        }));
        when(SchedulerState.ASSIGN,
                matchEvent(null, (container) -> {
                    assign(container);
                }));
        return super.receive();
    }

    private int hash(int tuple) {
        return 0;
    }

    private void sendSketch(Sketch sketch) {

    }

    private void refresh() {
        sketch = new Sketch();
        spaceSaving = new SpaceSaving(epsilon, theta);
    }

    private void assign(val container) {

    }
}
