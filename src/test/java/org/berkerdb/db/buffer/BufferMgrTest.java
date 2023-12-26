package org.berkerdb.db.buffer;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.LinkedList;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.berkerdb.db.file.Page.STRING_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BufferMgrTest {

    private static final int BUFFER_COUNT = 30;
    private static final String TEST = "test";
    private static final String TEST_TABLE = "testTable";

    @AfterEach
    void cleanAfter() throws IOException {
        //Delete the temp files.
        final String homeDirectory = System.getProperty("user.home");
        final Path homePath = Paths.get(homeDirectory);
        final Path dirPath = homePath.resolve(TEST);

        Files.deleteIfExists(dirPath.resolve("log_file"));
        Files.deleteIfExists(dirPath.resolve(TEST_TABLE));
        Files.deleteIfExists(dirPath);
    }

    @Test
    public void fundamentalBehaviourTest() {
        final NaiveBufferManager naiveBufferManager = new NaiveBufferManager(BUFFER_COUNT);
        final Block block = new Block(TEST_TABLE, 1);
        final LogManager logManager = new LogManager();

        final Buffer buffer = naiveBufferManager.pin(block);

        final long tx = 0L;
        final long lsn = logManager.append("testLog".getBytes());


        buffer.setString(TEST, 0, tx, lsn);
        buffer.flush(tx);

        assertEquals(buffer.getCurrentBlock(), block);
        assertEquals(TEST, buffer.getString(0));

            }

    @Test
    public void pinNewTest() {
        final NaiveBufferManager naiveBufferManager = new NaiveBufferManager(BUFFER_COUNT);
        final String fileName = TEST_TABLE;
        final LogManager logManager = new LogManager();

        final Buffer buffer = naiveBufferManager.pinNew(TEST_TABLE);

        final long tx = 0L;
        final long lsn = logManager.append("testLog".getBytes());


        buffer.setString(TEST, 0, tx, lsn);
        buffer.flush(tx);

        assertEquals(TEST, buffer.getString(0));
    }


    @Test
    public void unpinnedBufferDifferentAlgorithmsTest() {
        final var logManager = new LogManager();
        final var lruBufferManager = new LRUBufferManager(BUFFER_COUNT);

        final var checkStack = new LinkedList<Buffer>();

        for (int i = 0; i < BUFFER_COUNT; i++) {
            final var block = new Block(TEST_TABLE, i);
            final var buff = lruBufferManager.pin(block);
            final var demoVal = "DEMO TEXT " + i;
            final var demoLog = "DEMO LOG " + i + " " + LocalDateTime.now();

            final var lsn = logManager.append(demoLog.getBytes());
            buff.setString(demoVal, STRING_SIZE(demoVal.length()), i, lsn);
            lruBufferManager.unpin(block);
            checkStack.add(buff);
        }

        final var buffer = lruBufferManager.pin(new Block(TEST_TABLE,0));

        assertEquals(buffer.lastUnpinned, checkStack.getFirst().lastUnpinned);
    }

    @Test
    public void concurrentAccessTest() throws InterruptedException {
        final NaiveBufferManager naiveBufferManager = new NaiveBufferManager(BUFFER_COUNT);
        final LogManager logManager = new LogManager();
        final int threadCount = 35;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final BlockingQueue<Long> blockingQueue = new ArrayBlockingQueue<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int txNum = i;
            final int randBlockNum = ThreadLocalRandom.current().nextInt(0, 50);
            Thread.ofVirtual().start(() -> {
                try {
                    final Block block = new Block(TEST_TABLE, randBlockNum);
                    final Buffer buff = naiveBufferManager.pin(block);
                    final String log = "Thread Id: " + Thread.currentThread().threadId() + " block no: " + block.blockNumber() + " time: " + LocalDateTime.now();

                    final long lsn = logManager.append(log.getBytes());
                    final String content = "W";
                    final int offSet = txNum * STRING_SIZE(content.length());
                    blockingQueue.add(lsn);
                    buff.setString(content, offSet, txNum, lsn);
                    naiveBufferManager.unpin(block);
                    latch.countDown();
                } catch (RuntimeException e) {
                    assertTrue(e instanceof BufferPoolFullException);
                }
            });

        }
        latch.await();

        final var buff = naiveBufferManager.pin(new Block(TEST_TABLE, 0));
        buff.flush();

        assertEquals(threadCount, blockingQueue.size());
    }
}
