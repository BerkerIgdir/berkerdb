package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.io.IOException;

public class TransactionStartLogRecord implements LogRecord {

    private static final String TEMPLATE = "<START>";

    @Override
    public LogType getLogType() {
        return LogType.START;
    }

    @Override
    public long save() {
        return logManager.append(TEMPLATE.getBytes());
    }

    @Override
    public void undo(Transaction tx) {

    }


    @Override
    public void close() throws IOException {

    }
}
