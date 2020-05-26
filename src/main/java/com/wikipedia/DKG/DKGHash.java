package com.wikipedia.DKG;


import java.util.HashMap;

public class DKGHash {
    public final HashMap<String, Integer> mostFrequentMapping;
    public final CWHashFunction hashfunction;
    public final HashMap<Integer, Integer> hashFunctionMapping;

    /**
     *
     * This class encapsulates DKG's global mapping (Heavy Hitters to operator
     * instances and buckets of Sparse Items to instances)
     *
     * @param mostFrequentMapping
     *            Heavy Hitters to operator instances mapping
     * @param hashfunction
     *            the hash function that partitions the Sparse Items into
     *            buckets.
     * @param hashFunctionMapping
     *            buckets of Sparse Items to instances mapping
     */
    public DKGHash(HashMap<String, Integer> mostFrequentMapping, CWHashFunction hashfunction,
                   HashMap<Integer, Integer> hashFunctionMapping) {
        super();
        this.mostFrequentMapping = mostFrequentMapping;
        this.hashfunction = hashfunction;
        this.hashFunctionMapping = hashFunctionMapping;
    }

    /**
     * @param key
     *            a Heavy Hitter or Sparse Item key value.
     * @return the instances mapped to the provided Heavy Hitter or Sparse Item
     *         key.
     */
    public int map(String key) {
        if (mostFrequentMapping.containsKey(key)) {
            return mostFrequentMapping.get(key);
        }

        int x = hashfunction.hash(key);

        return hashFunctionMapping.get(x);

    }
}
