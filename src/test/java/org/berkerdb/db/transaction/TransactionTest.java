package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;
import org.berkerdb.db.log.LogManager;

import org.berkerdb.db.log.LogRecord;
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

import static org.junit.jupiter.api.Assertions.*;

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

    //    @Test
//    public void concurrentLockTableTest() throws InterruptedException {
//        final Block blockToOperateOn = new Block("test", 0);
//        final Block blockToOperateOn_1 = new Block("test", 1);
//
//        final CountDownLatch totalLatch = new CountDownLatch(3);
//
//        final Runnable writeOperation = () -> {
//            final Transaction writeTx = new Transaction();
//            writeTx.pin(blockToOperateOn);
//            writeTx.pin(blockToOperateOn_1);
//
//            writeTx.setInt(blockToOperateOn, 123, 0);
//            writeTx.setInt(blockToOperateOn_1, 123, 0);
//
//            writeTx.commit();
//            writeTx.flush();
//            totalLatch.countDown();
//        };
//
//        final Runnable readOperation = () -> {
//            final Transaction readTx = new Transaction();
//
//            readTx.pin(blockToOperateOn);
//            readTx.pin(blockToOperateOn_1);
//
//            readTx.getInt(blockToOperateOn, 0);
//            readTx.getInt(blockToOperateOn_1, 0);
//
//            readTx.commit();
//            readTx.flush();
//            totalLatch.countDown();
//        };
//
//        Thread.ofVirtual().name("Write Thread").start(writeOperation);
//        Thread.ofVirtual().name("Read Thread 1").start(readOperation);
//        Thread.ofVirtual().name("Read Thread 2").start(readOperation);
//
//
//        totalLatch.await();
//    }


    // This test, tests basically nothing, needs to be improved.
//    @Test
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
                    tx.pin(block);
                    tx.setInt(block, val.getAndIncrement(), off.addAndGet(Integer.BYTES));
                    tx.setStr(block, val.toString(), off.addAndGet(val.toString().length() + Integer.BYTES));
                    tx.commit();
                    lastAttemptedTxNum.set(tx.getCurrentTxNum());
                }
            } catch (Throwable e) {
                System.out.println(e.getMessage());
                System.out.println(STR."The last attempted tx number: \{lastAttemptedTxNum}");
            }
        };

        final Thread carrierThread = Thread.ofVirtual().name("Carrier Thread").start(writerThread);

        Thread.sleep(Duration.ofMillis(300));

        carrierThread.interrupt();
        final RecoveryManager recoveryManager = new RecoveryManager(new Transaction());

        recoveryManager.recovery();
        final LogManager logManager = new LogManager();

        for (LogRecord log : logManager) {
            if (log != null && log.getLogType().equals(LogRecord.LogType.COMMIT)) {
                assertEquals(log.getTxNum(), lastAttemptedTxNum.get());
                break;
            }
        }
    }

    //    @Test
    public void deadLockTest() throws InterruptedException {
        final Block blockToOperateOn = new Block("test", 0);
        final Block blockToOperateOn_1 = new Block("test", 1);

        final CountDownLatch totalLatch = new CountDownLatch(1);

        final Runnable writer_1 = () -> {
            final var tx = new Transaction();
            tx.pin(blockToOperateOn);
            tx.pin(blockToOperateOn_1);

            tx.setInt(blockToOperateOn, 123, 0);

            try {
                totalLatch.await();
                tx.setInt(blockToOperateOn_1, 123, 0);
            } catch (InterruptedException e) {
                return;
            }


            tx.commit();
            tx.flush();
        };

        final Runnable writer_2 = () -> {
            final var tx = new Transaction();
            tx.pin(blockToOperateOn);
            tx.pin(blockToOperateOn_1);

            tx.setInt(blockToOperateOn_1, 123, 0);

            try {
                totalLatch.await();
                tx.setInt(blockToOperateOn, 123, 0);
            } catch (Exception e) {
                return;
            }


            tx.commit();
            tx.flush();
        };

        final var thread_1 = Thread.ofVirtual().name("writer_1").start(writer_1);
        final var thread_2 = Thread.ofVirtual().name("writer_2").start(writer_2);
        Thread.sleep(1000);
        totalLatch.countDown();

        thread_1.join();
        thread_2.join();
    }


    // Read committed is a consistency model which strengthens read uncommitted by preventing dirty reads:
    // transactions are not allowed to observe writes from transactions which do not commit.
 //   @Test
    public void readCommittedLevelTest() {
        final var testVal = 9999;
        final var testOff = 0;
        final var blockToOperateOn = new Block("test", 0);

        final var t1 = new Transaction();
        t1.pin(blockToOperateOn);

        t1.setInt(blockToOperateOn, testVal, testOff);
        final var localVisT1 = t1.getInt(blockToOperateOn, testOff);

        //Local Visibility Check
        assertEquals(testVal, localVisT1);

        final var t2 = new Transaction();
        t2.pin(blockToOperateOn);

        final var localVisT2 = t2.getInt(blockToOperateOn, testOff);

        assertNotEquals(localVisT2, localVisT1);

        t1.commit();

        final var localVisT2AfterCommit = t2.getInt(blockToOperateOn, testOff);

        assertEquals(localVisT2AfterCommit, localVisT1);

        t2.commit();

    }

    @Test
    public void repeatableReadLevelTest() {
        // TO DO: IMPLEMENT
    }


    @Test
    public void MVCCLevelTest() {
        final var block = new Block("test", 0);
        final var t1 = new Transaction();
        final var t2 = new ReadOnlyTransaction();
        final var t3 = new Transaction();
        final var t4 = new ReadOnlyTransaction();

        t1.pin(block);
        t1.setInt(block, 999, 0);
        t1.commit();

        t3.pin(block);
        t3.setInt(block, 888, 0);

        t2.pin(block);
        final var t2Int = t2.getInt(block, 0);

        assertEquals(999, t2Int);

        t3.commit();

        final var t2Int_2 = t2.getInt(block, 0);
        assertEquals(999, t2Int_2);

        t2.commit();

        t4.pin(block);

        final var t4Int = t4.getInt(block, 0);
        assertEquals(888, t4Int);

        t4.commit();
    }

}
