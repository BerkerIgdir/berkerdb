package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.buffer.AbstractBufferManager;
import org.berkerdb.db.buffer.Buffer;
import org.berkerdb.db.file.Block;

import org.berkerdb.db.log.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Transaction {

    private static final AtomicLong TX_NUM = new AtomicLong(0);
    //    private final LogManager logManager = Main.DB().getLogManager();
    private final AbstractBufferManager bufferManager = Main.DB().getBufferManager();

    private final long currentTxNum;

    private final RecoveryManager recoveryManager = new RecoveryManager(this);

    //TO DO: Everything about this collection will be optimized in terms of performance.
    private final Map<Block, Buffer> blockBufferMap = new HashMap<>();

    public void start() {
        try (LogRecord logRecord = new TransactionStartLogRecord(TX_NUM.get())) {
            logRecord.save();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Transaction() {
        this.currentTxNum = TX_NUM.incrementAndGet();
    }

    public void pin(final Block block) {
        blockBufferMap.put(block, bufferManager.pin(block));
    }

    public void unpin(final Block block) {
        bufferManager.unpin(block);
        blockBufferMap.remove(block);
    }

    public void setInt(final Block block, final int val, final int off) {
        final Buffer buffer = blockBufferMap.getOrDefault(block, bufferManager.pin(block));

        if (buffer == null) {
            throw new RuntimeException("Buffer can not be reached without pinning");
        }

        final int oldVal = buffer.getInt(off);

        final long lsn;

        try (SetIntLogRecord logRecord = new SetIntLogRecord(block, currentTxNum, off, oldVal, val)) {
            lsn = logRecord.save();
        }

        buffer.setInt(val, off, currentTxNum, lsn);
    }

    public void setStr(final Block block, final String val, final int off) {
        final Buffer buffer = blockBufferMap.getOrDefault(block, bufferManager.pin(block));

        if (buffer == null) {
            throw new RuntimeException("Buffer can not be reached without pinning");
        }

        long lsn;

        try (SetStringLogRecord logRecord = new SetStringLogRecord(block, currentTxNum, off, buffer.getString(off), val)) {
            lsn = logRecord.save();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        buffer.setString(val, off, currentTxNum, lsn);
    }

    public void setBool(final Block block, final int off) {
        throw new RuntimeException("BOOL NOT SUPPORTED YET");
    }

    public int getInt(final Block block, final int off) {
        final Buffer buffer = blockBufferMap.getOrDefault(block, bufferManager.pin(block));

        return buffer.getInt(off);
    }

    public String getStr(final Block block, final int off) {
        final Buffer buffer = blockBufferMap.getOrDefault(block, bufferManager.pin(block));

        return buffer.getString(off);
    }

    public boolean getBool(final Block block) {
        return false;
    }


    public void commit() {
        recoveryManager.commit();
        blockBufferMap.keySet().forEach(bufferManager::unpin);
    }

    public void rollBack() {

        blockBufferMap.keySet().forEach(bufferManager::unpin);
    }

    public long getCurrentTxNum() {
        return currentTxNum;
    }

    void flush(){
        bufferManager.flush(currentTxNum);
    }

}
