package com.wikipedia.DKG;


import java.util.*;

/**
 * @author Nicolo Rivetti
 */
public class GreedyMultiProcessorScheduler {

    public final HashMap<String, Double> partitions;
    public final ArrayList<Instance> instances;
    public final TreeSet<String> ids;

    /**
     * Instantiate a Greedy Multi-Processor Scheduler to map the given set of
     * partitions to the given number of instances.
     *
     * @param partitions   a set of key partitions identifiers and frequencies.
     * @param instancesNum the number of available instances
     */
    public GreedyMultiProcessorScheduler(HashMap<String, Double> partitions, int instancesNum) {

        this.partitions = partitions;
        this.ids = new TreeSet<String>(new PartitionSorter(partitions));
        this.ids.addAll(partitions.keySet());

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
        for (Iterator<String> iterator = ids.iterator(); iterator.hasNext(); ) {
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

    class PartitionSorter implements Comparator<String> {

        Map<String, Double> base;

        public PartitionSorter(Map<String, Double> base) {
            this.base = base;
        }

        @Override
        public int compare(String a, String b) {
            if (base.get(a) > base.get(b)) {
                return -1;
            } else if (base.get(a) < base.get(b)) {
                return 1;
            } else {
                if (a.compareTo(b) < 0) {
                    return -1;
                } else if (a.compareTo(b) < 0) {
                    return 1;
                }
                return 0;
            }
        }
    }

}
