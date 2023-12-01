package org.berkerdb.db;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Buffer {
    private final Page page = new Page();
    private final AtomicInteger numberOfPins = new AtomicInteger(0);
    private final AtomicReference<Block> currentBlock = new AtomicReference<>();
    private final AtomicLong modifiedBy = new AtomicLong(-1);
    private final AtomicLong lastLogLsn = new AtomicLong(0);

    public void setString(final String s, final int off, final long tx, final long lsn) {
        if (lsn < 0) {
            return;
        }
        page.setStr(off, s);
        modifiedBy.set(tx);
        lastLogLsn.set(lsn);
    }

    public String getString(final int off) {
        return page.getStr(off);
    }

    public void assignNew(final String fileName) {
        final int lastBlockNum = page.lastBlockNum(fileName);
        final Block currBlock = new Block(fileName, lastBlockNum);
        page.read(currBlock);
        currentBlock.updateAndGet(_ -> currBlock);
        numberOfPins.set(0);
    }

    public void flush(final long tx) {
        if (modifiedBy.get() <= tx) {
            page.write(currentBlock.get());
        }
    }

    void assignToBlock(final Block block) {

        final Block curBlock = currentBlock.get();
        currentBlock.updateAndGet(_ -> block);
        page.read(currentBlock.get());
        numberOfPins.updateAndGet(_ -> 0);
    }

    public void pin() {
        numberOfPins.incrementAndGet();
    }

    public void unpin() {
        numberOfPins.decrementAndGet();
    }

    public boolean isPinned() {
        return numberOfPins.get() > 0;
    }

    public Block getCurrentBlock() {
        return currentBlock.get();
    }
}
