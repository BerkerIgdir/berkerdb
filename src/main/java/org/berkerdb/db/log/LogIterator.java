package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;
import static org.berkerdb.db.log.LogManager.LAST_POS;

public class LogIterator implements Iterator<LogRecord> {
    private final Page page = new Page();
    private Block block;
    private int currentRecord;

    public LogIterator(final Block block) {
        this.block = block;
        initPage();
    }

    @Override
    public boolean hasNext() {
        return !(block.blockNumber() == 0 && currentRecord == BLOCK_SIZE);
    }

    @Override
    public LogRecord next() {
        if (!hasNext()) {
            return null;
        }

        byte[] bytes = Optional.ofNullable(page.getByteArray(currentRecord)).orElseGet(() -> new byte[]{});

        if (bytes.length == 0) {
            block = new Block(block.fileName(), block.blockNumber() - 1);
            initPage();
            bytes = page.getByteArray(currentRecord);
        }
        
        currentRecord += (bytes.length + Integer.BYTES);

        if (currentRecord + bytes.length > BLOCK_SIZE && hasNext()) {
            block = new Block(block.fileName(), block.blockNumber() - 1);
            initPage();
        }

        return resolveLogType(bytes);
    }


    private LogRecord resolveLogType(final byte[] bytes) {
        final var memorySegment = MemorySegment.ofArray(bytes);
        try (final var arena = Arena.ofConfined()) {
            final var memSeg = arena.allocate(bytes.length);
            MemorySegment.copy(memorySegment, 0, memSeg, 0, bytes.length);
            final int logType = memSeg.get(ValueLayout.JAVA_INT, 0);

            final LogRecord.LogType logTypeEnum = LogRecord.LogType.fromInt(logType);

            return switch (logTypeEnum) {
                case START -> new TransactionStartLogRecord(bytes);
                case COMMIT -> new CommitLogRecord(bytes);
                case SET_INT -> new SetIntLogRecord(bytes);
                case SET_STRING -> new SetStringLogRecord(bytes);
                default -> throw new IllegalArgumentException();
            };
        }
    }

    private void initPage() {
        page.read(block);
        currentRecord = page.getInt(LAST_POS);
    }
}
