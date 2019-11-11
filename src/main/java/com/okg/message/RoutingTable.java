package com.okg.message;

import java.util.HashMap;

/**
 * Routing Table
 */
public class RoutingTable<K, V> extends Entity {

    private final HashMap<K, V> map;

    public RoutingTable(HashMap<K, V> map) {
        this.map = map;
    }

    public boolean containsKey(int key) {
        return map.containsKey(key);
    }

    public V get(Object key) {
        return map.get(key);
    }

}
