package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.util.Arrays;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class LogManager implements Iterable<LogRecord> {

    private static final int LAST_POS = 0;
    private static final String LOG_FILE = "log_file";
    private final Page page;
    private int currentPosition;
    private int currentBlockNum;

    private final AtomicLong lsnCount = new AtomicLong(0);

    public LogManager() {
        this.page = new Page();
        currentBlockNum = page.lastBlockNum(LOG_FILE);
        page.read(new Block(LOG_FILE, currentBlockNum));
    }

    public synchronized long append(final byte[] bytes) {
        final int sizeOfRecord = bytes.length;
        if (currentPosition + sizeOfRecord > Page.BLOCK_SIZE) {
            page.append(LOG_FILE);
            currentBlockNum++;
            page.read(new Block(LOG_FILE, currentBlockNum));
            currentPosition = 0;
        }
        page.setByteArray(currentPosition, bytes);
        currentPosition += sizeOfRecord;
        finalizeAppend();
        return lsnCount.incrementAndGet();
    }

    public synchronized long append(final Object[] objects) {
        final int sizeOfRecord = Arrays.stream(objects).mapToInt(this::size).sum();

        if (currentPosition + sizeOfRecord > Page.BLOCK_SIZE) {
            page.append(LOG_FILE);
            currentBlockNum++;
            page.read(new Block(LOG_FILE, currentBlockNum));
            currentPosition = 0;
        }

        for (var o : objects) {
            if (o instanceof Integer i) {
                page.setInt(currentPosition, i);
            } else if (o instanceof String s) {
                page.setStr(currentPosition, s);
            } else {
                //Will be replaced in the future
                throw new RuntimeException("Only supported types are String and Integer for logging!");
            }
        }

        currentPosition += sizeOfRecord;

        finalizeAppend();
        return lsnCount.incrementAndGet();
    }

    private void finalizeAppend() {
        page.setInt(currentPosition, page.getInt(LAST_POS));
        page.setInt(LAST_POS, currentPosition + Page.INT_SIZE);
    }


    private int size(Object o) {
        if (o instanceof Integer) {
            return Page.INT_SIZE;
        }
        if (o instanceof String) {
            return Page.STRING_SIZE(((String) o).length());
        }
        //Will be replaced in the future
        throw new RuntimeException("Only supported types are String and Integer for logging!");
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return null;
    }
}
