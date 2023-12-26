package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Objects;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;


public class LogManager implements Iterable<Record> {

    static final int LAST_POS = 0;
    static final String LOG_FILE = "log_file";
    private final Page page;
    private int currentPosition;
    private int currentBlockNum;

    private long lsnCount;

    public LogManager() {
        this.page = new Page();
        currentBlockNum = page.lastBlockNum(LOG_FILE);
        currentPosition = page.getInt(LAST_POS);
        page.read(new Block(LOG_FILE, currentBlockNum));
        if (page.getFileSize(LOG_FILE) == 0) {
            initPage();
        }
    }

    public synchronized long append(final byte[] bytes) {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            throw new RuntimeException("Object array can not be null/empty");
        }

        final int sizeOfRecord = bytes.length + Integer.BYTES;
        if (currentPosition - sizeOfRecord < 0) {
            page.append(LOG_FILE);
            currentBlockNum++;
            page.read(new Block(LOG_FILE, currentBlockNum));
            initPage();
        }

        currentPosition -= sizeOfRecord;
        page.setByteArray(currentPosition, bytes);
        page.setInt(LAST_POS, currentPosition);
        return lsnCount++;
    }

    public synchronized long append(final Object[] objects) {
        if (Objects.isNull(objects) || objects.length == 0) {
            throw new RuntimeException("Object array can not be null/empty");
        }

        for (var o : objects) {
            if (o instanceof String s) {
                return append(s.getBytes());
            }
            if (o instanceof Integer i) {
                return append(BigInteger.valueOf(i).toByteArray());
            }
            throw new RuntimeException(String.format("Object type %s not supported for logging", o.toString()));
        }
        return 0L;
    }

    @Override
    public Iterator<Record> iterator() {
        flush();
        return new LogIterator(new Block(LOG_FILE, currentBlockNum));
    }

    private void flush() {
        page.write(new Block(LOG_FILE, currentBlockNum));
    }

    private void initPage() {
        currentPosition = BLOCK_SIZE;
        page.setInt(LAST_POS, currentPosition);

    }

    public synchronized int blockCount() {
        return page.lastBlockNum(LOG_FILE);
    }

    public void flush(final long lsn) {
        if (lsn >= lsnCount) {
            flush();
        }
    }

}
