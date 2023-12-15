package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.Page;

import java.util.Iterator;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;


public class LogManager implements Iterable<LogRecord> {

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

    @Override
    public Iterator<LogRecord> iterator() {
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
