package com.okg.message;

import java.util.HashMap;

public class Sketch extends Entity{

    private final HashMap<Integer, Integer> M;   // mapping from keys of heavy hitters to their frequencies
    private final static int m = 100000;
    private final int[] A;    // array for mapping frequencies to buckets by hash function

    public Sketch() {
        this(new HashMap<>(), new int[m]);
    }

    public Sketch(HashMap<Integer, Integer> M, int[] A) {
        this.M = M;
        this.A = A;
    }

    public HashMap<Integer, Integer> getM() {
        return M;
    }

    public int[] getA() {
        return A;
    }
}
