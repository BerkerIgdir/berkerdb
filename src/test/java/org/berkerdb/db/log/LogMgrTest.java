package org.berkerdb.db.log;

import org.berkerdb.Main;
import org.berkerdb.db.file.Block;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.berkerdb.db.log.LogManager.LOG_FILE;
import static org.berkerdb.db.log.LogRecordMemoryLayout.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogMgrTest {

    @AfterEach
    void cleanAfter() throws IOException {
        //Delete the temp files.
        final String homeDirectory = System.getProperty("user.home");
        final Path homePath = Paths.get(homeDirectory);
        final Path dirPath = homePath.resolve("test");
        Files.deleteIfExists(dirPath.resolve(LOG_FILE));
    }

    @Test
    public void basicMemTest() {
        final var memLay = MemoryLayout.structLayout(
                BASIC_LAYOUT,
                ValueLayout.JAVA_INT.withName("OLD_VAL_LENGTH"),
                MemoryLayout.sequenceLayout(2, ValueLayout.JAVA_CHAR).withName("FILENAME")
        );


        final var recType = 123;
        final var txNum = 12L;
        final var blockNum = 11;
        final var fileNameLen = 13;
        final var oldValOff = 14;


        final var oldValLengthHand = memLay.varHandle(MemoryLayout.PathElement.groupElement("OLD_VAL_LENGTH"));
        final var fileNameHand = memLay.varHandle(MemoryLayout.PathElement.groupElement("FILENAME"), MemoryLayout.PathElement.sequenceElement());

        try (final Arena arena = Arena.ofConfined()) {
            final MemorySegment memorySegment = arena.allocate(memLay);
            LogRecordMemoryLayout.setBasicLayout(memorySegment,
                    recType,
                    txNum,
                    blockNum,
                    fileNameLen,
                    oldValOff);


            oldValLengthHand.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, 1234);
            fileNameHand.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, 0L, 'd');
            fileNameHand.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, 1L, 'b');

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void setIntLogRecordTest() {
        final Block block = new Block("junk", 1);
        final LogManager logManager = Main.DB().getLogManager();

        final long oldTx = 0L;
        final int off = 1;
        final int oldVal = 0;
        final int newVal = 111;

        try (SetIntLogRecord logRecord = new SetIntLogRecord(block, oldTx, off, oldVal, newVal)) {
            logRecord.save();
        }

        final var iterator = logManager.iterator();

        try (SetIntLogRecord record = (SetIntLogRecord) iterator.next()) {
            assertEquals(block.blockNumber(), record.getBlockNum());
            assertEquals(oldVal, record.getOldVal());
            assertEquals(block.fileName().length(), record.getFilenameLength());
            assertEquals(oldTx, record.getTxNum());
            assertEquals(newVal, record.getNewVal());
            assertEquals(block.fileName(), record.getFilename());
        }

    }

    @Test
    public void setStringLogRecordTest() {
        final Block block = new Block("junk", 1);
        final LogManager logManager = Main.DB().getLogManager();

        final long oldTx = 0L;
        final int off = 1;
        final String oldVal = "OLD_VAL";
        final String newVal = "NEW_VAL";

        try (SetStringLogRecord logRecord = new SetStringLogRecord(block, oldTx, off, oldVal, newVal)) {
            logRecord.save();
        }

        final var iterator = logManager.iterator();

        try (SetStringLogRecord record = (SetStringLogRecord) iterator.next()) {
            assertEquals(block.blockNumber(), record.getBlockNum());
            assertEquals(oldVal, record.getOldVal());
            assertEquals(block.fileName().length(), record.getFilenameLength());
            assertEquals(oldTx, record.getTxNum());
            assertEquals(newVal, record.getNewVal());
            assertEquals(block.fileName(), record.getFilename());
        }

    }
}
