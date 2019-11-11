package com.okg.actor;

import akka.actor.AbstractFSM;
import com.okg.message.Sketch;
import com.okg.state.CoordinatorState;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public class CoordinatorActor extends AbstractFSM<CoordinatorState, MessageQueue> {

    private static final int k = 5;

    {
        startWith(CoordinatorState.WAIT_ALL, null);
    }



    @Override
    public PartialFunction<Object, BoxedUnit> receive() {

        when(CoordinatorState.WAIT_ALL,
                matchEvent(Sketch.class, (sketch, container) -> stay())
                        .event(Sketch.class,
                                (sketch, container) -> {
                                    container.add(sketch);
                                    if (container.size() == k) {
                                        goTo(CoordinatorState.GENERATION);
                                    } else {
                                        return stay();
                                    }
                                }));
        when(CoordinatorState.GENERATION, );
        return super.receive();
    }
}
