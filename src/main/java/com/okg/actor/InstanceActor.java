package com.okg.actor;

import akka.actor.AbstractFSM;
import com.okg.state.InstanceState;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public class InstanceActor extends AbstractFSM<InstanceState, MessageQueue> {

    {
        startWith(InstanceState.RUN, null);
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {

        return super.receive();
    }
}
