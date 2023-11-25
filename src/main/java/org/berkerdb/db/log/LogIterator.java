package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.util.Iterator;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;
import static org.berkerdb.db.log.LogManager.LAST_POS;

public class LogIterator implements Iterator<LogRecord> {
    private final Page page = new Page();
    private Block block;
    private int currentRecord;

    public LogIterator(final Block block) {
        this.block = block;
        initPage();
    }

    @Override
    public boolean hasNext() {
        return !(currentRecord == BLOCK_SIZE && block.blockNumber() == 0);
    }

    @Override
    public LogRecord next() {
        if (!hasNext()) {
            return null;
        }

        final byte[] bytes = page.getByteArray(currentRecord);
        currentRecord += (bytes.length + Integer.BYTES);
        System.out.println("Current Rec: " + currentRecord + " block " + block.blockNumber());
        if (currentRecord + bytes.length > BLOCK_SIZE && hasNext()) {
            block = new Block(block.fileName(), block.blockNumber() - 1);
            initPage();
        }

        return new LogRecord(new String(bytes));
    }

    private void initPage() {
        page.read(block);
        currentRecord = page.getInt(LAST_POS);
    }
}
