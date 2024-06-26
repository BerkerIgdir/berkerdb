package org.berkerdb.db.file;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PageTest {

    final String test = "test";
    final String testFile = "test_file";

    @AfterEach
    void removeDirAndFiles() throws IOException {
        //Delete the temp files.
        final String homeDirectory = System.getProperty("user.home");
        final Path homePath = Paths.get(homeDirectory);
        final Path dirPath = homePath.resolve(test);
        Files.deleteIfExists(dirPath.resolve(testFile));
        Files.deleteIfExists(dirPath.resolve("log_file"));
        Files.deleteIfExists(dirPath);
    }

    @Test
    public void pageFundamentalFunctionTest() {
        final Page page = new Page();
        final Block block = new Block(testFile, 0);
        final int testVal = 555;

        page.setStr(0, test);
        page.setInt(20, testVal);
        page.setBool(25, false);
        page.setByteArray(26, test.getBytes());
//        page.setByteArrarayToMemory(0,test.getBytes());

        page.write(block);
        page.read(block);

        assertEquals(testVal, page.getInt(20));
        assertEquals(test, page.getStr(0));
        assertEquals(test, new String(page.getByteArray(26)));
        assertFalse(page.getBool(25));
//        assertEquals(test,new String(page.getByteArrayFromMemory(0)));
    }

    //    @Test
    public void fileManagerMultiThreadedAccess() throws InterruptedException {
        final FileManager fileManager = new FileManager(test);
        final Page page = new Page();
        final Block block = new Block(testFile, 0);

        final int j = 2;
        page.setInt(0, j);
        final Lock lock = new ReentrantLock();
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            Thread.ofVirtual().start(() -> {
                lock.lock();
                int l = page.getInt(0);
                ++l;
                page.setInt(0, l);
                latch.countDown();
                lock.unlock();
            });
        }


        latch.await(3, TimeUnit.SECONDS);
        final int k = page.getInt(0);
        assertEquals(k, 12);
        assertEquals(11, fileManager.getBlockWriteCount());
    }
}
