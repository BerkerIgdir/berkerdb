package org.berkerdb;

import org.berkerdb.db.BerkerDB;
import org.berkerdb.db.buffer.LRUBufferManager;
import org.berkerdb.db.log.LogManager;


public class Main {

    private static BerkerDB DB;

    public static void main(String[] args) {
        if (args == null || args[0] == null || args[0].isEmpty()) {
            System.out.println("Db name can not be null/empty!");
            System.exit(1);
        }
        DB = new BerkerDB(args[0]);
    }

    public static BerkerDB DB() {
        //TO DO: Change this to a better structured solution.
        //This is enough for now for development purposes.
        if (DB == null) {
            DB = new BerkerDB("test");
            DB.setLogManager(new LogManager());
            DB.setBufferManager(new LRUBufferManager(400));
            return DB;
        }

        //for testing purposes
        return DB;
    }

}