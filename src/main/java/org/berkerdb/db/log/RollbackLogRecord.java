package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.io.IOException;

public class RollbackLogRecord implements LogRecord{
    @Override
    public LogType getLogType() {
        return null;
    }

    @Override
    public long save() {
        return 0;
    }

    @Override
    public void undo(Transaction tx) {

    }


    @Override
    public void close() throws IOException {

    }
}
