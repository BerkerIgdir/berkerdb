package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static org.berkerdb.db.log.LogRecordMemoryLayout.*;

public class SetIntLogRecord implements LogRecord {


    private final String TEMPLATE;

    private final VarHandle FILENAME_HANDLE;
    private final VarHandle OLD_VAL_HANDLE;
    private final VarHandle NEW_VAL_HANDLE;
    private final MemoryLayout SET_INT_MEMORY_LAYOUT;

    private final Arena MEMORY_ARENA = Arena.ofShared();
    private final MemorySegment MEMORY_SEGMENT;

    public SetIntLogRecord(final Block block, final long tx,
                           final int off, final int oldVal, final int newVal) {


        this.SET_INT_MEMORY_LAYOUT = LogRecordMemoryLayout.createIntRecordMemoryLayout(block.fileName().length());
        this.MEMORY_SEGMENT = MEMORY_ARENA.allocate(SET_INT_MEMORY_LAYOUT);

        LogRecordMemoryLayout.setBasicLayout(MEMORY_SEGMENT,
                getLogType().ordinal(),
                tx,
                block.blockNumber(),
                block.fileName().length(),
                off);

        this.OLD_VAL_HANDLE = SET_INT_MEMORY_LAYOUT.varHandle(groupElement(OLD_VAL));
        this.NEW_VAL_HANDLE = SET_INT_MEMORY_LAYOUT.varHandle(groupElement(NEW_VAL));
        this.FILENAME_HANDLE = SET_INT_MEMORY_LAYOUT.varHandle(groupElement(FILENAME));

        OLD_VAL_HANDLE.set(oldVal);
        NEW_VAL_HANDLE.set(newVal);
        FILENAME_HANDLE.set(block.fileName());

        this.TEMPLATE = STR. "<SET_STRING \{ block.fileName() }, \{ block.blockNumber() }, \{ tx }, \{ off },\{ oldVal }, \{ newVal }>" ;
    }

    @Override
    public LogType getLogType() {
        return LogType.SET_INT;
    }

    @Override
    public long save() {
        return logManager.append(MEMORY_SEGMENT.toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public void undo(final Transaction tx) {
        final String fileName = (String) FILENAME_HANDLE.get(MEMORY_SEGMENT);
        final int blockNum = (int) BLOCK_NUMBER_HANDLE.get(MEMORY_SEGMENT);
        final int oldVal = (int) OLD_VAL_HANDLE.get(MEMORY_SEGMENT);
        final int off = (int) OLD_VAL_OFF_HANDLE.get(MEMORY_SEGMENT);
        final Block block = new Block(fileName, blockNum);

        tx.pin(block);
        tx.setInt(block, oldVal, off);
        tx.unpin(block);
    }

    @Override
    public String toString() {
        return TEMPLATE;
    }

    @Override
    public void close() {
        MEMORY_ARENA.close();
    }
}
