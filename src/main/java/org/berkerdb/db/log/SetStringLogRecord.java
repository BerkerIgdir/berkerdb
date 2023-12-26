package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static org.berkerdb.db.log.LogRecordMemoryLayout.*;

public class SetStringLogRecord implements LogRecord {


    private final long tx;
    private final Block block;
    private final int off;
    private final String oldVal;
    private final String newVal;

    private final String TEMPLATE;

    private final MemoryLayout SET_STRING_MEMORY_LAYOUT;
    private final VarHandle FILENAME_HANDLE;
    private final VarHandle OLD_VAL_HANDLE;
    private final VarHandle NEW_VAL_HANDLE;

    public SetStringLogRecord(final Block block, final long tx, final int off, final String oldVal, final String newVal) {
        this.tx = tx;
        this.block = block;
        this.off = off;
        this.oldVal = oldVal;
        this.newVal = newVal;

        this.SET_STRING_MEMORY_LAYOUT = LogRecordMemoryLayout.createStringRecordMemoryLayout(block.fileName().length(), oldVal.length(), newVal.length());
        this.FILENAME_HANDLE = SET_STRING_MEMORY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement(FILENAME), MemoryLayout.PathElement.sequenceElement());
        this.OLD_VAL_HANDLE = SET_STRING_MEMORY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement(OLD_VAL), MemoryLayout.PathElement.sequenceElement());
        this.NEW_VAL_HANDLE = SET_STRING_MEMORY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement(NEW_VAL), MemoryLayout.PathElement.sequenceElement());

        this.TEMPLATE = STR. "<SET_STRING \{ block.fileName() }, \{ block.blockNumber() }, \{ tx }, \{ off },\{ oldVal }, \{ newVal }>" ;
    }

    @Override
    public long save() {
        try (final Arena arena = Arena.ofConfined()) {
            final MemorySegment memorySegment = arena.allocate(SET_STRING_MEMORY_LAYOUT);
            System.out.println(SET_STRING_MEMORY_LAYOUT.byteAlignment());
            LogRecordMemoryLayout.setBasicLayout(memorySegment,
                    123,
                    tx,
                    block.blockNumber(),
                    block.fileName().length(),
                    off);

            final String fileName = block.fileName();

            for (int i = 0; i < fileName.length(); i++) {
                FILENAME_HANDLE.set(memorySegment, i, fileName.charAt(i));
            }

            for (int i = 0; i < oldVal.length(); i++) {
                OLD_VAL_HANDLE.set(memorySegment, i, oldVal.charAt(i));
            }

            for (int i = 0; i < newVal.length(); i++) {
                NEW_VAL_HANDLE.set(memorySegment, i, newVal.charAt(i));
            }

            return logManager.append(memorySegment.toArray(ValueLayout.JAVA_BYTE));
        } catch (Exception e) {
            throw new RuntimeException("Memory Segmentation Fault!", e);
        }
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setStr(block, oldVal, off);
        tx.unpin(block);
    }


    @Override
    public LogType getLogType() {
        return LogType.SET_STRING;
    }

    @Override
    public String toString() {
        return TEMPLATE;
    }

    @Override
    public void close() throws IOException {

    }
}
