package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.io.IOException;

public class CommitLogRecord implements LogRecord {

    private final String TEMPLATE;

    public CommitLogRecord(final long tx) {
        this.TEMPLATE = STR. "<COMMIT,\{ tx }>" ;
    }

    public long save() {
        return logManager.append(TEMPLATE.getBytes());
    }

    @Override
    public void undo(Transaction tx) {

    }

    @Override
    public LogType getLogType() {
        return LogType.COMMIT;
    }

    @Override
    public String toString() {
        return TEMPLATE;
    }

    @Override
    public void close() throws IOException {

    }
}
