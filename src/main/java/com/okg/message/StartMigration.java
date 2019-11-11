package com.okg.message;

import java.util.HashMap;

public class StartMigration extends Event {

    private final HashMap<Integer, Pair> migrationTable;

    public StartMigration(HashMap<Integer, Pair> migrationTable) {
        this.migrationTable = migrationTable;
    }

    public HashMap<Integer, Pair> getMigrationTable() {
        return migrationTable;
    }

    private class Pair {
        private final int before;
        private final int after;

        public Pair(int before, int after) {
            this.before = before;
            this.after = after;
        }

        public int getBefore() {
            return before;
        }

        public int getAfter() {
            return after;
        }
    }
}
