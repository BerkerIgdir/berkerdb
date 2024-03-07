package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.berkerdb.db.log.LogRecordMemoryLayout.*;

public class SetIntLogRecord implements LogRecord {


    // A record will only be processed by one thread.
    private final Arena MEMORY_ARENA = Arena.ofConfined();
    private final MemorySegment MEMORY_SEGMENT;
    private final static int NEW_VAL_OFF = 28;
    private final static int FILENAME_OFF = 32;

    public SetIntLogRecord(final Block block, final long tx,
                           final int off, final int oldVal, final int newVal) {

        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(FILENAME_OFF + block.fileName().length() + 1);
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, LOG_TYPE_OFF, LogType.SET_INT.getNumber());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, BLOCK_NUM_OFF, block.blockNumber());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, FILENAME_LENGTH_OFF, block.fileName().length());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, OFF_OFFSET, off);
        MEMORY_SEGMENT.set(ValueLayout.JAVA_LONG, TX_NUM_OFF, tx);
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, OLD_VAL_OFF, oldVal);
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, NEW_VAL_OFF, newVal);

        MEMORY_SEGMENT.setUtf8String(FILENAME_OFF, new String(block.fileName().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
    }

    public SetIntLogRecord(final byte[] bytes) {
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(bytes.length);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, MEMORY_SEGMENT, 0, bytes.length);
    }

    @Override
    public LogType getLogType() {
        return LogType.SET_INT;
    }

    public int getBlockNum() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, BLOCK_NUM_OFF);
    }

    public int getOldVal() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OLD_VAL_OFF);
    }

    public int getFilenameLength() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, FILENAME_LENGTH_OFF);
    }

    public long getTxNum() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_LONG, TX_NUM_OFF);
    }

    public int getNewVal() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, NEW_VAL_OFF);
    }

    public String getFilename() {
        return MEMORY_SEGMENT.getUtf8String(FILENAME_OFF);
    }

    public int getOff() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OFF_OFFSET);
    }

    @Override
    public long save() {
        return logManager.append(MEMORY_SEGMENT.toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public void undo(final Transaction tx) {
        final Block block = new Block(getFilename(), getBlockNum());

        tx.pin(block);
        tx.setInt(block, getOldVal(), getOff());
        tx.unpin(block);
    }

    @Override
    public String toString() {
        return STR."<SET_INT \{MEMORY_SEGMENT.getUtf8String(FILENAME_OFF)}, \{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, BLOCK_NUM_OFF)}, \{MEMORY_SEGMENT.get(ValueLayout.JAVA_LONG, TX_NUM_OFF)},\{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OFF_OFFSET)}, \{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OLD_VAL_OFF)},\{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, NEW_VAL_OFF)}>";
    }

    @Override
    public void close() {
        MEMORY_ARENA.close();
    }
}
