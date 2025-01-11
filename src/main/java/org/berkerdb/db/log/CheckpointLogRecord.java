package org.berkerdb.db.log;

import org.berkerdb.db.transaction.Transaction;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.berkerdb.db.log.LogRecordMemoryLayout.*;

public class CheckpointLogRecord implements LogRecord {

    private static final String TEMPLATE = "<CHECKPOINT>";

    private static final int TX_ARRAY_LENGTH_OFF = LOG_TYPE_OFF + Integer.BYTES;
    private static final int TX_ARRAY_OFF = TX_ARRAY_LENGTH_OFF + Integer.BYTES;

    private final Arena MEMORY_ARENA = Arena.ofConfined();
    private final MemorySegment MEMORY_SEGMENT;


    @Override
    public LogType getLogType() {
        return LogType.CHECKPOINT;
    }

    @Override
    public long save() {
        return logManager.append(MEMORY_SEGMENT.toArray(ValueLayout.JAVA_BYTE));
    }

    public CheckpointLogRecord(final long[] txNums) {
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate((long) txNums.length * Long.BYTES + Integer.BYTES + Integer.BYTES);

        //Log Type
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, LOG_TYPE_OFF, LogType.CHECKPOINT.getNumber());
        //Array Length
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, TX_ARRAY_LENGTH_OFF, txNums.length);
        MemorySegment.copy(MemorySegment.ofArray(txNums), 0L, MEMORY_SEGMENT, TX_ARRAY_OFF, (long) txNums.length * Long.BYTES);
    }

    //TO DO: Test it!
    public CheckpointLogRecord(final byte[] bytes) {
        this.MEMORY_SEGMENT = MemorySegment.ofArray(bytes).asReadOnly();
    }

    @Override
    public void undo(Transaction tx) {
        throw new RuntimeException("Illegal operation for this particular record type!");
    }

    @Override
    public long getTxNum() {
        return 0;
    }

    public long[] getTxNumbers() {
        return MEMORY_SEGMENT.asSlice(TX_ARRAY_OFF,
                MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, TX_ARRAY_LENGTH_OFF)).toArray(ValueLayout.JAVA_LONG);
    }

    @Override
    public void close() {
        MEMORY_ARENA.close();
    }

    @Override
    public String toString() {
        return TEMPLATE;
    }
}
