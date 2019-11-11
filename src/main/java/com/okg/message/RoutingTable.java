package com.okg.message;

import java.util.HashMap;

public class RoutingTable extends Entity {

    private final HashMap<Integer, Integer> routingTable;

    public RoutingTable(HashMap<Integer, Integer> routingTable) {
        this.routingTable = routingTable;
    }

    public HashMap<Integer, Integer> getRoutingTable() {
        return routingTable;
    }
}
