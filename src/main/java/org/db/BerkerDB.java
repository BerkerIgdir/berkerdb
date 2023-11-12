package org.db;

import org.db.file.FileManager;

public class BerkerDB {

    public final FileManager fileManager;
    public BerkerDB(String dbName){
        fileManager = new FileManager(dbName);
    }
}
