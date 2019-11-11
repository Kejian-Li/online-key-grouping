package com.okg.message;

import java.util.HashMap;

public class Sketch extends Entity {

    private final HashMap<Integer, Integer> M;   // mapping for heavy hitters
    private final static int m = 100000;
    private final int[] A;          // array for sparse items

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
