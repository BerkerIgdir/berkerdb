package org.berkerdb.db.log;

import org.berkerdb.Main;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

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
    public void fundamentalBehaviourTest() {
        final LogManager logManager = Main.DB().getLogManager();
        final String testLog = "TEST LOG";
        final String testLog_1 = "TEST LOG1";
        final String testLog_2 = "TEST LOG11";

        logManager.append(testLog.getBytes());
        logManager.append(testLog_1.getBytes());
        logManager.append(testLog_2.getBytes());

        final var iterator = logManager.iterator();

        final var recordLog_2 = iterator.next();
        final var recordLog_1 = iterator.next();
        final var recordLog = iterator.next();

        assertEquals(testLog, recordLog.record());
        assertEquals(testLog_1, recordLog_1.record());
        assertEquals(testLog_2, recordLog_2.record());
    }

    @Test
    public void logMgrBlockOverFlow() {
        final LogManager logManager = new LogManager();
        final String testLog = "TEST LOG";
        int recSize = testLog.length() + Integer.BYTES;
        int recCount = 0;
        while (recSize < 600) {
            logManager.append(testLog.getBytes());
            recSize += 12;
            recCount++;
        }
        final var iterator = logManager.iterator();
        Record logRecord;
        int readCount = 0;

        while (iterator.hasNext()) {
            logRecord = iterator.next();
            assertEquals(testLog, logRecord.record());
            readCount++;
        }

        assertEquals(recCount, readCount);
    }

    //    @RepeatedTest(10)
    @Test
    public void concurrentAccessTest() throws InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used Memory before " + usedMemoryBefore);
        System.out.println("Total amount of cores " + runtime.availableProcessors());
        final LogManager logManager = new LogManager();
        final int numberOfThreads = 100;

        final CountDownLatch latch = new CountDownLatch(numberOfThreads);
        final BlockingQueue<Long> queue = new ArrayBlockingQueue<>(numberOfThreads);
        Thread.ofVirtual().start(() -> {
            while (true) {
                for (var record : logManager) {
                    final var startTime = Instant.now();
                    final String testLog = "Test Log Reading Thread: " + Thread.currentThread().threadId();
                    final var endTime = Instant.now();
//                    System.out.println(testLog + " " + (endTime.toEpochMilli() - startTime.toEpochMilli()));
                }
            }
        });

        for (int i = 0; i < numberOfThreads; i++) {
            Thread.ofVirtual().start(() -> {
                final var startTime = Instant.now();
                final String testLog = "Test Log Writing Thread: " + Thread.currentThread().threadId();
                final var lsn = logManager.append(testLog.getBytes());
                queue.add(lsn);
                latch.countDown();
                final var endTime = Instant.now();
//                System.out.println(testLog + " " + (endTime.toEpochMilli() - startTime.toEpochMilli()));
            });
        }

        latch.await();

        assertEquals(numberOfThreads, queue.size());
        long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Memory increased: " + (usedMemoryAfter - usedMemoryBefore));
    }

    @Test
    public void basicMemTest() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used Memory before " + usedMemoryBefore);
        System.out.println("Total amount of cores " + runtime.availableProcessors());
        final var memLay = MemoryLayout.structLayout(
                BASIC_LAYOUT,
                ValueLayout.JAVA_INT.withName("OLD_VAL_LENGTH"),
                MemoryLayout.sequenceLayout(2, ValueLayout.JAVA_CHAR).withName("FILENAME")
        );

        final var oldValLengthHand = memLay.varHandle(MemoryLayout.PathElement.groupElement("OLD_VAL_LENGTH"));

        final var fileNameHand = memLay.varHandle(MemoryLayout.PathElement.groupElement("FILENAME"), MemoryLayout.PathElement.sequenceElement());
        try (final Arena arena = Arena.ofConfined()) {
            final MemorySegment memorySegment = arena.allocate(memLay);
            long usedMemoryAfterMemSeg = runtime.totalMemory() - runtime.freeMemory();
            System.out.println("Used Memory after Seg " + usedMemoryAfterMemSeg);
            LogRecordMemoryLayout.setBasicLayout(memorySegment,
                    123,
                    12L,
                    11,
                    13,
                    14);


            oldValLengthHand.set(memorySegment, 1234);
            fileNameHand.set(memorySegment, 0L, 'd');
            fileNameHand.set(memorySegment, 1L, 'b');

            final var v1 = (int) RECORD_TYPE_HANDLE.get(memorySegment);
            final var v2 = (long) TX_NUM_HANDLE.get(memorySegment);
            final var v3 = (int) BLOCK_NUMBER_HANDLE.get(memorySegment);
            final var v4 = (int) FILENAME_LENGTH_HANDLE.get(memorySegment);
            final var v5 = (int) OLD_VAL_OFF_HANDLE.get(memorySegment);
            final var v6 = (int) oldValLengthHand.get(memorySegment);
            final var v7 = fileNameHand.get(memorySegment, 0L);
            final var v8 = fileNameHand.get(memorySegment, 1L);

            memorySegment.byteSize();
            long usedMemoryAfterMemSeg1 = runtime.totalMemory() - runtime.freeMemory();
            System.out.println("Used Memory after Seg " + usedMemoryAfterMemSeg1);
            System.out.println(" Memory Seg " + memorySegment.byteSize());
        } catch (Exception e) {
            throw new RuntimeException("Memory Segmentation Fault!", e);
        }
        long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Memory increased: " + (usedMemoryAfter - usedMemoryBefore));
        System.out.println("Memory increased: " + usedMemoryAfter);
    }
}
