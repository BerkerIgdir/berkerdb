package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.io.IOException;

public class CheckpointLogRecord implements LogRecord {

    private static final String TEMPLATE = "<CHECKPOINT>";

    @Override
    public LogType getLogType() {
        return LogType.CHECKPOINT;
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
