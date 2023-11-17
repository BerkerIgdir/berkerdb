package org.berkerdb.db.log;

import org.berkerdb.db.file.Page;

import java.util.Iterator;

public class LogRecord implements Iterator<Record>{
    private final Page page;

    public LogRecord(final Page page) {
        this.page = page;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Record next() {
        return null;
    }
}
