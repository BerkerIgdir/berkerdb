package org.berkerdb;

import org.berkerdb.db.BerkerDB;

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
        //for testing purposes
        return new BerkerDB("test");
    }

}