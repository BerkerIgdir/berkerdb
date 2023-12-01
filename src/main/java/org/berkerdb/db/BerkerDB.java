package org.berkerdb.db;

import org.berkerdb.db.file.FileManager;

public class BerkerDB {

    private final FileManager fileManager;


    public BerkerDB(String dbName) {
        fileManager = new FileManager(dbName);
    }

    public FileManager getFileManager() {
        return fileManager;
    }
}
