package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.buffer.AbstractBufferManager;
import org.berkerdb.db.buffer.Buffer;
import org.berkerdb.db.file.Block;

import org.berkerdb.db.log.LogRecord;
import org.berkerdb.db.log.SetIntLogRecord;
import org.berkerdb.db.log.SetStringLogRecord;
import org.berkerdb.db.log.TransactionStartLogRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class Transaction {

    private static final AtomicLong TX_NUM = new AtomicLong(0);
    //    private final LogManager logManager = Main.DB().getLogManager();
    private final AbstractBufferManager bufferManager = Main.DB().getBufferManager();

    //Eveything about this collection will be optimized in terms of performance.
    private final Set<Buffer> buffers = new HashSet<>();
    private final Map<Block, Buffer> blockBufferMap = new HashMap<>();

    public void start() {
        final LogRecord logRecord = new TransactionStartLogRecord();
        logRecord.save();
    }

    public void pin(final Block block) {
        final Buffer buff = blockBufferMap.getOrDefault(block, bufferManager.pin(block));
        buffers.add(buff);
    }

    public void unpin(final Block block) {
        bufferManager.unpin(block);
        blockBufferMap.remove(block);
    }

    public void setInt(final Block block, final int val, final int off) {
        final Buffer buffer = blockBufferMap.get(block);

        if (buffer == null) {
            throw new RuntimeException("Buffer can be reached without pinning");
        }

        final int oldVal = buffer.getInt(off);
        final long currentTxNum = TX_NUM.incrementAndGet();
        final SetIntLogRecord logRecord = new SetIntLogRecord(block, currentTxNum, off, oldVal, val);
        final long lsn = logRecord.save();
        buffer.setInt(val, off, currentTxNum, lsn);
    }

    public void setStr(final Block block, final String val, final int off) {
        final Buffer buffer = blockBufferMap.get(block);

        if (buffer == null) {
            throw new RuntimeException("Buffer can be reached without pinning");
        }

        final long tx = TX_NUM.incrementAndGet();

        final SetStringLogRecord logRecord = new SetStringLogRecord(block, tx, off, buffer.getString(off), val);
        final long lsn = logRecord.save();

        buffer.setString(val, off, tx, lsn);
    }

    public void setBool(final Block block) {
    }

    public int getInt(final Block block) {

        return 0;
    }

    public String getStr(final Block block) {
        return null;
    }

    public boolean getBool(final Block block) {
        return false;
    }


    public void commit() {
        buffers.stream().map(Buffer::getCurrentBlock).forEach(bufferManager::unpin);
    }

    public void rollBack() {

    }


}
