package com.wikipedia.DKG;


import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DKG_Builder {

    private final HashMap<String, Double> mostFrequent;
    private final int k;

    private final CWHashFunction hashFunction;
    private final double[] hashFunctionDistrib;

    private final SpaceSaving<String> ss;

    /**
     * This class implements the learning and building phase of DKG
     *
     * @param theta         double value in (0,1], heavy hitter threshold, i.e., all keys
     *                      with an empirical probability larger than or equal to theta
     *                      belong to the heavy hitter set.
     * @param epsilonFactor defines the ration between the Space Saving's Heavy Hitter
     *                      threshold theta and precision parameter epsilon: epsilon =
     *                      theta / epsilonFactor.
     * @param k             the number of available instances
     * @param factor        double value >=1, set the number of buckets of sparse items to
     *                      factor * k (number of available instances).
     */
    public DKG_Builder(double theta, double epsilonFactor, int k, double factor) {
        this.k = k;
        this.mostFrequent = new HashMap<String, Double>();

        int codomain = (int) Math.ceil(k * factor);

        RandomDataGenerator uniformGenerator = new RandomDataGenerator();
        uniformGenerator.reSeed(1000);

        long prime = 10000019;
        long a = uniformGenerator.nextLong(1, prime - 1);
        long b = uniformGenerator.nextLong(1, prime - 1);

        this.hashFunction = new CWHashFunction(codomain, prime, a, b);
        this.hashFunctionDistrib = new double[codomain];

        this.ss = new SpaceSaving(theta / epsilonFactor, theta);
    }

    /**
     * Updates DKG's data structures with key, ie, performs the learning phase
     * of DKG.
     *
     * @param key
     */

    public void newSample(String key) {
        ss.newSample(key);
        hashFunctionDistrib[hashFunction.hash(key)]++;
    }

    /**
     * Returns a DKGHash objects that encapsulate a close the optimal mapping
     * built given the updates from the learning phase.
     *
     * @return a DKGHash object thath encapsulates DKG's global mapping (Heavy
     * Hitters to operator instances and buckets of Sparse Items to
     * instances)
     */
    public DKGHash build() {

        HashMap<String, Integer> ssMap = ss.getHeavyHitters();


        for (Map.Entry<String, Integer> entry : ssMap.entrySet()) {
            hashFunctionDistrib[hashFunction.hash(entry.getKey())] -= entry.getValue();
        }


        // put heavy hitters ,  key is string
        for (Map.Entry<String, Integer> entry : ssMap.entrySet()) {
            this.mostFrequent.put(entry.getKey(), (double) entry.getValue());
        }

        // put buckets , key is number
        for (int i = 0; i < hashFunctionDistrib.length; i++) {
            mostFrequent.put(String.valueOf(i), hashFunctionDistrib[i]);
        }

        GreedyMultiProcessorScheduler mps = new GreedyMultiProcessorScheduler(mostFrequent, k);

        ArrayList<Instance> instances = mps.run();

        HashMap<String, Integer> mostFrequentMapping = new HashMap<String, Integer>();
        HashMap<Integer, Integer> hashFunctionMapping = new HashMap<Integer, Integer>();

        for (Instance instance : instances) {
            for (String item : instance.getPartitions()) {
                if(item.length() < 3) {
                    // from buckets to index
                    hashFunctionMapping.put(Integer.parseInt(item), instance.id);
                } else {
                    // from heavy hitter to index
                    mostFrequentMapping.put(item, instance.id);
                }

            }

        }


        return new DKGHash(mostFrequentMapping, hashFunction, hashFunctionMapping);

    }
}
