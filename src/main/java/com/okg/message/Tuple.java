package com.okg.message;

public class Tuple extends Entity {

    private final int key;

    public Tuple(int key) {
        this.key = key;
    }

    public int getKey() {
        return key;
    }
}
