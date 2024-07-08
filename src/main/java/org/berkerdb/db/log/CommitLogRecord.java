package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.berkerdb.db.log.LogRecordMemoryLayout.*;

public class CommitLogRecord implements LogRecord {

    private final Arena MEMORY_ARENA = Arena.ofConfined();
    private final MemorySegment MEMORY_SEGMENT;

    public CommitLogRecord(final long tx) {
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(Integer.SIZE + Long.BYTES);

        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, LOG_TYPE_OFF,getLogType().getNumber());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_LONG, TX_NUM_OFF, tx);
    }

    public CommitLogRecord(final byte[] bytes){
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(bytes.length);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, MEMORY_SEGMENT, 0, bytes.length);
    }

    @Override
    public long save() {
        return logManager.append(MEMORY_SEGMENT.toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public void undo(Transaction tx) {
        throw new RuntimeException("START TRANSACTION CAN NOT BE UNDONE");
    }

    @Override
    public LogType getLogType() {
        return LogType.COMMIT;
    }

    @Override
    public String toString() {
        return "<SET_COMMIT %d>".replace("%d",Long.toString(getTxNum()));
    }
    public long getTxNum() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_LONG, TX_NUM_OFF);
    }

    @Override
    public void close() {
        MEMORY_ARENA.close();
    }
}
