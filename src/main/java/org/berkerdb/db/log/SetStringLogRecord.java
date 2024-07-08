package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;


import static org.berkerdb.db.log.LogRecordMemoryLayout.*;


public class SetStringLogRecord implements LogRecord {


    private final Arena MEMORY_ARENA = Arena.ofConfined();
    private final MemorySegment MEMORY_SEGMENT;

    public SetStringLogRecord(final Block block, final long tx, final int off, final String oldVal, final String newVal) {
        final int oldValOff = NEW_VAL_LENGTH_OFF + Integer.BYTES;
        final int newValOff = oldValOff + oldVal.length();
        final int fileNameOff = newValOff + newVal.length();

        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(fileNameOff + block.fileName().length());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, LOG_TYPE_OFF, LogType.SET_STRING.getNumber());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, BLOCK_NUM_OFF, block.blockNumber());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, FILENAME_LENGTH_OFF, block.fileName().length());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, OFF_OFFSET, off);
        MEMORY_SEGMENT.set(ValueLayout.JAVA_LONG, TX_NUM_OFF, tx);
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, OLD_VAL_LENGTH_OFF, oldVal.length());
        MEMORY_SEGMENT.set(ValueLayout.JAVA_INT, NEW_VAL_LENGTH_OFF, newVal.length());


        final MemorySegment oldValSeg = MemorySegment.ofArray(oldVal.getBytes(StandardCharsets.UTF_8));
        final MemorySegment newValSeg = MemorySegment.ofArray(newVal.getBytes(StandardCharsets.UTF_8));
        final MemorySegment fileNameMemSeg = MemorySegment.ofArray(block.fileName().getBytes(StandardCharsets.UTF_8));

        MemorySegment.copy(oldValSeg, 0, MEMORY_SEGMENT, oldValOff, oldValSeg.byteSize());
        MemorySegment.copy(newValSeg, 0, MEMORY_SEGMENT, newValOff, newValSeg.byteSize());
        MemorySegment.copy(fileNameMemSeg, 0, MEMORY_SEGMENT, fileNameOff, fileNameMemSeg.byteSize());
    }

    public SetStringLogRecord(final byte[] bytes) {
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(bytes.length);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, MEMORY_SEGMENT, 0, bytes.length);
    }

    @Override
    public long save() {
        return logManager.append(MEMORY_SEGMENT.toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public void undo(Transaction tx) {
        final Block block = new Block(getFilename(), getBlockNum());

        tx.pin(block);
        tx.setStr(block, getOldVal(), getOff());
        tx.unpin(block);
    }


    public int getBlockNum() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, BLOCK_NUM_OFF);
    }


    public int getOff() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OFF_OFFSET);
    }

    public String getOldVal() {
        final int oldValOff = NEW_VAL_LENGTH_OFF + Integer.BYTES;
        return new String(MEMORY_SEGMENT.asSlice(oldValOff, MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OLD_VAL_LENGTH_OFF)).toArray(ValueLayout.JAVA_BYTE));
    }

    public String getNewVal() {
        final int oldValOff = NEW_VAL_LENGTH_OFF + Integer.BYTES;
        final int newValOff = MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OLD_VAL_LENGTH_OFF) + oldValOff;
        return new String(MEMORY_SEGMENT.asSlice(newValOff, MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, NEW_VAL_LENGTH_OFF)).toArray(ValueLayout.JAVA_BYTE));
    }

    public int getFilenameLength() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, FILENAME_LENGTH_OFF);
    }

    public long getTxNum() {
        return MEMORY_SEGMENT.get(ValueLayout.JAVA_LONG, TX_NUM_OFF);
    }


    public String getFilename() {
        final int oldValOff = NEW_VAL_LENGTH_OFF + Integer.BYTES;
        final int newValOff = MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OLD_VAL_LENGTH_OFF) + oldValOff;
        final int fileNameOff = MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, NEW_VAL_LENGTH_OFF) + newValOff;
        return new String(MEMORY_SEGMENT.asSlice(fileNameOff, MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, FILENAME_LENGTH_OFF)).toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public LogType getLogType() {
        return LogType.SET_STRING;
    }

    @Override
    public String toString() {
        return STR."<SET_STRING \{getFilename()}, \{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, BLOCK_NUM_OFF)}, \{MEMORY_SEGMENT.get(ValueLayout.JAVA_LONG, TX_NUM_OFF)},\{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OFF_OFFSET)}, \{MEMORY_SEGMENT.get(ValueLayout.JAVA_INT, OLD_VAL_OFF)},\{getNewVal()}>";
    }

    @Override
    public void close() {
        MEMORY_ARENA.close();
    }
}
