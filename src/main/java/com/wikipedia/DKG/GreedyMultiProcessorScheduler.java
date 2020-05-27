package com.wikipedia.DKG;


import java.util.*;

/**
 * @author Nicolo Rivetti
 */
public class GreedyMultiProcessorScheduler {

    public final HashMap<String, Double> partitions;
    public final ArrayList<Instance> instances;

    public final ArrayList<String> keys = new ArrayList<>();
    /**
     * Instantiate a Greedy Multi-Processor Scheduler to map the given set of
     * partitions to the given number of instances.
     *
     * @param partitions   a set of key partitions identifiers and frequencies.
     * @param instancesNum the number of available instances
     */
    public GreedyMultiProcessorScheduler(HashMap<String, Double> partitions, int instancesNum) {

        this.partitions = partitions;

        ArrayList<Pair> keysList = new ArrayList<>();
        for(Map.Entry<String, Double> entry : partitions.entrySet()) {
            keysList.add(new Pair(entry.getKey(), entry.getValue()));
        }
        Collections.sort(keysList, new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                if(o1.load - o2.load > 0) {
                    return -1;
                }else if(o1.load - o2.load < 0) {
                    return 1;
                }else {
                    return 0;
                }
            }
        });

        for (Pair pair : keysList) {
            keys.add(pair.key);
        }


        instances = new ArrayList<Instance>();

        for (int i = 0; i < instancesNum; i++) {
            instances.add(new Instance(i));
        }
    }

    /**
     * Runs the Multi-Processor Scheduling algorithm.
     *
     * @return A set of Instance objects that encapsulate the mapping from key
     * partitions to instances.
     */
    public ArrayList<Instance> run() {
        for (Iterator<String> iterator = keys.iterator(); iterator.hasNext(); ) {
            String id = iterator.next();

            getEmptiestInstance().addLoad(id, partitions.get(id));
        }

        return instances;
    }

    private Instance getEmptiestInstance() {
        Instance target = null;
        for (Instance instance : instances) {

            if (target == null) {
                target = instance;
            } else if (target.getLoad() > instance.getLoad()) {
                target = instance;
            }

        }
        return target;
    }

    private class Pair {
        String key;
        double load;

        public Pair(String key, double load) {
            this.key = key;
            this.load = load;
        }
    }


}
