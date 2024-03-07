package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LockTable {

    private static final long MAX_WAIT_TIME = Duration.ofSeconds(10).toMillis();

    //-1 represents an exclusive lock
    private final Map<Block, Integer> blockLockCountMap = new HashMap<>();


    public synchronized void getSharedLock(final Block block) {
        final long currentTime = System.currentTimeMillis();
        final int currentLockCount = Optional.ofNullable(blockLockCountMap.get(block)).orElse(0);
        while (currentLockCount < 0 && (System.currentTimeMillis() - currentTime >= MAX_WAIT_TIME)) {
            try {
                wait(MAX_WAIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        blockLockCountMap.put(block, currentLockCount + 1);
    }

    public synchronized void getXLock(final Block block) {
        final long currentTime = System.currentTimeMillis();
        final int currentLock = Optional.ofNullable(blockLockCountMap.get(block)).orElse(0);
        while (currentLock != 0 && (System.currentTimeMillis() - currentTime >= MAX_WAIT_TIME)) {
            try {
                wait(MAX_WAIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        blockLockCountMap.put(block, -1);
    }

    public synchronized void sRelease(final Block block) {
        if (blockLockCountMap.get(block) <= 0) {
            return;
        }
        blockLockCountMap.put(block, blockLockCountMap.get(block) - 1);
    }

    public synchronized void xRelease(final Block block) {
        if (blockLockCountMap.get(block) >= 0) {
            return;
        }
        blockLockCountMap.put(block, 0);
        notifyAll();
    }


}