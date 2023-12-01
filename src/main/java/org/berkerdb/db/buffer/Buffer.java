package org.berkerdb.db.buffer;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

public class Buffer {
    final Page page = new Page();

    private long tx = -1;
    private long lastLogLsn;
    private Block currentBlock;
    private int numberOfPins;

    public void setString(final String s, final int off, final long tx, final long lsn) {
        if (lsn < 0) {
            return;
        }
        page.setStr(off, s);
        this.tx = tx;
        this.lastLogLsn = lsn;
    }

    public String getString(final int off) {
        return page.getStr(off);
    }

    public void append(final String s, final long tx, final long lsn) {
        if (lsn < 0) {
            return;
        }
        page.append(s);
        this.tx = tx;
        this.lastLogLsn = lsn;
    }

    void assignNew(final String fileName) {
        flush();
        final int lastBlockNum = page.lastBlockNum(fileName);
        final Block currBlock = new Block(fileName, lastBlockNum);
        page.read(currBlock);
        this.currentBlock = currBlock;
        numberOfPins = 0;
    }

    public void flush(final long tx) {
        if (this.tx <= tx) {
            page.write(currentBlock);
        }
    }

    void flush() {
        if (this.tx >= 0) {
            page.write(currentBlock);
        }
        this.tx = -1;
    }

    void assignToBlock(final Block block) {
        flush();
        page.read(block);
        this.currentBlock = block;

        numberOfPins = 0;
    }

    void pin() {
        numberOfPins++;
    }

     void unpin() {
        numberOfPins--;
    }

    boolean isPinned() {
        return numberOfPins > 0;
    }

    Block getCurrentBlock() {
        return currentBlock;
    }
}
