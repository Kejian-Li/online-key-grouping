package com.okg.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Nicolo Rivetti
 */
public class SpaceSaving {

    private final double theta;

    private final HashMap<Integer, Elem> list;
    private final int lSizeLimit;

    private final TreeMap<Integer, HashSet<Integer>> minSets;

    private int min = 1;
    // number of items
    private double m = 0;

    private static final double DEFAULT_EPSILON = 0.05;
    private static final double DEFAULT_THETA = 0.1;

    public SpaceSaving() {
        this(DEFAULT_EPSILON, DEFAULT_THETA);
    }

    /**
     *
     * This class implements the Space Saving algorithm presented by Metwally,
     * Agrawal and El Abbadi in "Efficient computation of frequent and top-k
     * elements in data streams" (ICDT'05).
     *
     * @param epsilon
     *            double value in (0,theta], precision parameter setting the
     *            memory footprint, ie, the algorithm stores 1/epsilon entries.
     * @param theta
     *            double value in (0,1], heavy hitter threshold, i.e., all keys
     *            with an empirical probability larger than or equal to theta
     *            belong to the heavy hitter set.
     */
    public SpaceSaving(double epsilon, double theta) {

        this.theta = theta;

        this.lSizeLimit = (int) Math.ceil(1.0 / epsilon);

        this.list = new HashMap<Integer, Elem>((int) Math.ceil(1 + lSizeLimit / 0.75));

        this.minSets = new TreeMap<Integer, HashSet<Integer>>();
    }

    /**
     * Updated the Space Saving algorithm with an identifier.
     *
     * @param identifier
     */
    public void newSample(int identifier) {

        m++;

        if (list.containsKey(identifier)) {
            int count = list.get(identifier).count;

            minSets.get(count).remove(identifier);
            if (minSets.get(count).isEmpty()) {
                minSets.remove(count);
            }

            list.get(identifier).count++;
            count++;

            if (!minSets.containsKey(count)) {
                minSets.put(count, new HashSet<Integer>((int) Math.ceil(1 + lSizeLimit / 0.75)));
            }

            minSets.get(count).add(identifier);

        } else if (list.size() < lSizeLimit) {
            list.put(identifier, new Elem(1, 0));

            if (!minSets.containsKey(1)) {
                minSets.put(min, new HashSet<Integer>((int) Math.ceil(1 + lSizeLimit / 0.75)));
            }

            minSets.get(1).add(identifier);

        } else {
            int victimKey = minSets.firstEntry().getValue().iterator().next();
            Elem victimEntry = list.get(victimKey);

            list.remove(victimKey);
            minSets.firstEntry().getValue().remove(victimKey);

            if (minSets.firstEntry().getValue().isEmpty()) {
                minSets.remove(minSets.firstEntry().getKey());
            }

            list.put(identifier, new Elem(victimEntry.count + 1, victimEntry.count));

            if (!minSets.containsKey(victimEntry.count + 1)) {
                minSets.put(victimEntry.count + 1, new HashSet<Integer>((int) Math.ceil(1 + lSizeLimit / 0.75)));
            }

            minSets.get(victimEntry.count + 1).add(identifier);

        }

    }

    private class Elem {
        // count
        private int count;
        // last evicted count
        private int e;

        public Elem(int count, int e) {
            this.count = count;
            this.e = e;
        }

    }

    /**
     * @param identifier
     * @return true if the identifier belongs to the Heavy Hitter set.
     */
    public boolean isHeavyHitter(int identifier) {
        Elem elem = list.get(identifier);
        return elem.count - elem.e >= Math.ceil(theta * m);
    }

    /**
     * @return a <Identifier, Frequency> map containing all the identifiers of
     *         the Heavy Hitters and their frequency estimation
     */
    public HashMap<Integer, Integer> getHeavyHitters() {
        HashMap<Integer, Integer> res = new HashMap<Integer, Integer>();
        for (Entry<Integer, Elem> entry : list.entrySet()) {
            if (isHeavyHitter(entry.getKey())) {
                Elem elem = entry.getValue();
                res.put(entry.getKey(), elem.count - elem.e);
            }
        }
        return res;
    }

}
