package org.berkerdb.db;

import org.berkerdb.db.buffer.AbstractBufferManager;

import org.berkerdb.db.file.FileManager;
import org.berkerdb.db.log.LogManager;

public class BerkerDB {

    private static final int BUFFER_SIZE = 400;

    private final FileManager fileManager;
    private AbstractBufferManager bufferManager;
    private LogManager logManager;


    public BerkerDB(final String dbName) {
        fileManager = new FileManager(dbName);
    }

    public void setBufferManager(AbstractBufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public AbstractBufferManager getBufferManager() {
        return bufferManager;
    }

    public LogManager getLogManager() {
        return logManager;
    }

}
