package com.okg.actor;

import akka.actor.AbstractFSM;
import com.okg.message.*;
import com.okg.state.SchedulerState;
import com.okg.util.SpaceSaving;
import com.okg.util.TwoUniversalHash;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class SchedulerActor extends AbstractFSM<SchedulerState, MessageQueue> {


    private static int N = 1000;
    private static int m = 100000;
    private int n = 0;

    private static final int k = 5;
    private SpaceSaving spaceSaving;
    private int[] A;
    private TwoUniversalHash hashFunction;

    private RoutingTable<Integer, Integer> routingTable;

    private static final double epsilon = 0.05;
    private static final double theta = 0.1;


    {
        startWith(SchedulerState.HASH, null);
        spaceSaving = new SpaceSaving(epsilon, theta);
        A = new int[k];
        initializeHashFunction();
    }

    private void initializeHashFunction() {
        int codomain = (int) Math.ceil(k);

        RandomDataGenerator uniformGenerator = new RandomDataGenerator();
        uniformGenerator.reSeed(1000);

        long prime = 10000019;
        long a = uniformGenerator.nextLong(1, prime - 1);
        long b = uniformGenerator.nextLong(1, prime - 1);
        this.hashFunction = new TwoUniversalHash(codomain, prime, a, b);
    }


    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        when(SchedulerState.HASH,
                matchEvent(Tuple.class, (tuple, container) -> {
                    n++;
                    int index = hash(tuple.getKey());

                    if (n == N) {
                        n = 0;    // reuse
                        goTo(SchedulerState.COLLECT);
                    }
                }));
        when(SchedulerState.COLLECT,
                matchEvent(Tuple.class, (tuple, container) -> {
                    n++;
                    int key = tuple.getKey();
                    container.add(key);

                    spaceSaving.newSample(key);
                    int index = hash(key);
                    A[index]++;

                    if (n == m) {
                        container.add(new Barrier());  // add barrier

                        makeAndSendSketch(A);
                        // clear
                        n = 0;
                        for (int i = 0; i < k; i++) {
                            A[i] = 0;
                        }
                        spaceSaving = new SpaceSaving(epsilon, theta);

                        goTo(SchedulerState.WAIT);
                    }
                }));

        when(SchedulerState.WAIT,
                matchEvent(Tuple.class, (tuple, container) -> {
                    n++;
                    int key = tuple.getKey();
                    container.add(key);
                    spaceSaving.newSample(key);
                    int index = hash(key);
                    A[index]++;
                })
                        .event(RoutingTable.class, (routingTable) -> {
                            this.routingTable = routingTable;
                            goTo(SchedulerState.ASSIGN);
                        }));
        when(SchedulerState.ASSIGN,
                matchEvent(null, (container) -> {
                    assign(container);
                }));
        return super.receive();
    }

    private int hash(int key) {
        return hashFunction.hash(key);
    }

    private void makeAndSendSketch(int[] A) {
        HashMap<Integer, Integer> heavyHitters = spaceSaving.getHeavyHitters();  // M
        for (Map.Entry<Integer, Integer> entry : heavyHitters.entrySet()) {
            int key = entry.getKey();
            int frequency = entry.getValue();
            int index = hash(key);
            A[index] -= frequency;
        }
        Sketch sketch = new Sketch(heavyHitters, A);

        self().tell(sketch, self());
    }

    private void assign(LinkedList<Message> container) {
        Message message = container.pollFirst();
        while (!(message instanceof Barrier)) {
            assign(message);
            message = container.pollFirst();
        }
    }

    private int assign(Message message) {
        Tuple tuple = (Tuple) message;
        int key = tuple.getKey();
        if (routingTable.containsKey(key)) {
            return routingTable.get(key);
        } else {
            int index = hash(key);
            return index;
        }
    }
}
