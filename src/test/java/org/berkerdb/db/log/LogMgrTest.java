package org.berkerdb.db.log;

import org.berkerdb.Main;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import static org.berkerdb.db.log.LogManager.LOG_FILE;
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
        LogRecord logRecord;
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
                    System.out.println(testLog + " " + (endTime.toEpochMilli() - startTime.toEpochMilli()));
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
                System.out.println(testLog + " " + (endTime.toEpochMilli() - startTime.toEpochMilli()));
            });
        }

        latch.await();

        assertEquals(numberOfThreads, queue.size());

    }
}
