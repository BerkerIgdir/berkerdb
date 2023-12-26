package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.util.Iterator;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;
import static org.berkerdb.db.log.LogManager.LAST_POS;

public class LogIterator implements Iterator<Record> {
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
    public Record next() {
        if (!hasNext()) {
            return null;
        }

        final byte[] bytes = page.getByteArray(currentRecord);
        currentRecord += (bytes.length + Integer.BYTES);

        if (currentRecord + bytes.length > BLOCK_SIZE && hasNext()) {
            block = new Block(block.fileName(), block.blockNumber() - 1);
            initPage();
        }

        return new Record(new String(bytes));
    }

    private void initPage() {
        page.read(block);
        currentRecord = page.getInt(LAST_POS);
    }
}
