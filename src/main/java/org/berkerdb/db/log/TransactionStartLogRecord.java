package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.berkerdb.db.log.LogRecordMemoryLayout.LOG_TYPE_OFF;
import static org.berkerdb.db.log.LogRecordMemoryLayout.TX_NUM_OFF;

public class TransactionStartLogRecord implements LogRecord {

    private static final String TEMPLATE = "<START>";

    private final Arena MEMORY_ARENA = Arena.ofConfined();
    private final MemorySegment MEMORY_SEGMENT;


    public TransactionStartLogRecord(final long tx){
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(Integer.SIZE + Long.BYTES);

        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, LOG_TYPE_OFF,getLogType().getNumber());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_LONG, TX_NUM_OFF, tx);
    }
    @Override
    public LogType getLogType() {
        return LogType.START;
    }

    @Override
    public long save() {
        return logManager.append(MEMORY_SEGMENT.toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public void undo(Transaction tx) {
        throw new RuntimeException("START TRANSACTION CAN NOT BE UNDONE");
    }

    public long getTxNum() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_LONG, TX_NUM_OFF);
    }

    @Override
    public void close() {
        MEMORY_ARENA.close();
    }
}
