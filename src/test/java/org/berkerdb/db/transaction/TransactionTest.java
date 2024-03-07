package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;
import org.berkerdb.db.log.LogManager;

import org.berkerdb.db.log.LogRecord;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TransactionTest {


    @AfterEach
    void cleanAfter() throws IOException {
        //Delete the temp files.
        final String homeDirectory = System.getProperty("user.home");
        final Path homePath = Paths.get(homeDirectory);
        final Path dirPath = homePath.resolve("test");
        Files.deleteIfExists(dirPath.resolve("test"));
        Files.deleteIfExists(dirPath.resolve("log_file"));
        Files.deleteIfExists(dirPath);
    }

    @Test
    public void fundamentalBehaviourTest() {
        final Transaction underTest = new Transaction();
        final Block block = new Block("test", 0);
        final int testVal = 123;
        underTest.pin(block);
        underTest.setInt(block, testVal, 0);

        underTest.setInt(block, testVal + 3, 4);
        underTest.setStr(block, "demoVal", 8);

        underTest.commit();

        final Page page = new Page();
        page.read(block);

        assertEquals(page.getInt(0), testVal);
        assertEquals(page.getInt(4), testVal + 3);
        assertEquals(page.getStr(8), "demoVal");

        final LogManager logManager = new LogManager();

        for (var record : logManager) {
            assertNotNull(record);
        }
    }

    @Test
    public void concurrentLockTableTest() throws InterruptedException {
        final Block blockToOperateOn = new Block("test", 0);
//        final CountDownLatch writeLatch = new CountDownLatch(1);
        final CountDownLatch totalLatch = new CountDownLatch(3);

        final Runnable writeOperation = () -> {
            final Transaction writeTx = new Transaction();
            writeTx.pin(blockToOperateOn);
            writeTx.setInt(blockToOperateOn, 123, 0);

            writeTx.commit();
            writeTx.flush();
            totalLatch.countDown();
        };

        final Runnable readOperation = () -> {
            final Transaction readTx = new Transaction();

            readTx.pin(blockToOperateOn);
            final var num = readTx.getInt(blockToOperateOn, 0);

            readTx.commit();
            readTx.flush();
            totalLatch.countDown();
        };

        Thread.ofVirtual().name("Write Thread").start(writeOperation);
        Thread.ofVirtual().name("Read Thread 1").start(readOperation);
        Thread.ofVirtual().name("Read Thread 2").start(readOperation);


        totalLatch.await();
    }

    @Test
    public void recoveryTest() throws InterruptedException {
        final Block block = new Block("test", 0);

        //Atomic variables due to nature of lambda.
        final AtomicInteger off = new AtomicInteger(0);
        final AtomicInteger val = new AtomicInteger(0);
        final AtomicLong lastAttemptedTxNum = new AtomicLong();


        final Runnable writerThread = () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    final Transaction tx = new Transaction();
                    lastAttemptedTxNum.set(tx.getCurrentTxNum());
                    tx.pin(block);
                    tx.setInt(block, val.getAndIncrement(), off.addAndGet(Integer.BYTES));
                    tx.setStr(block, val.toString(), off.addAndGet(val.toString().length() + Integer.BYTES));
                    tx.commit();
                }
            } catch (Throwable e) {
                System.out.println(lastAttemptedTxNum);
            }
        };
        final Thread carrierThread = Thread.ofVirtual().name("Carrier Thread").start(writerThread);
//        carrierThread.join();
        Thread.sleep(Duration.ofMillis(500));

        try {
            carrierThread.interrupt();
        }
        catch (Throwable e){
            System.out.println(e);
        }
        final RecoveryManager recoveryManager = new RecoveryManager(new Transaction());
        recoveryManager.recovery();
        final LogManager logManager = new LogManager();
        final LogRecord logRecord;
        for (LogRecord log : logManager) {
            if (log != null && log.getLogType().equals(LogRecord.LogType.COMMIT)) {
                System.out.println(log);
                break;
            }
        }
    }

    @Test
    public void deadLockTest() {

    }

    @Test
    public void MVCCBehaviourTest() {

    }
}
