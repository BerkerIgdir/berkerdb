package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.buffer.AbstractBufferManager;
import org.berkerdb.db.buffer.Buffer;
import org.berkerdb.db.file.Block;

import org.berkerdb.db.log.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Transaction {

    protected static final AtomicLong TX_NUM = new AtomicLong(0);
    //    private final LogManager logManager = Main.DB().getLogManager();
    protected final AbstractBufferManager bufferManager = Main.DB().getBufferManager();
    protected final long currentTxNum;
    protected final RecoveryManager recoveryManager = new RecoveryManager(this);
    protected final ConcurrencyManager concurrencyManager = new ConcurrencyManager(this);
    static final AtomicBoolean isTxAllowed = new AtomicBoolean(true);
    static final Set<Transaction> activeTxs = Collections.synchronizedSet(new HashSet<>());

    //TO DO: Everything about this collection will be optimized in terms of performance.
    protected final Map<Block, Buffer> blockBufferMap = new HashMap<>();

    public static Optional<Transaction> getNewTx() {
        if (!isTxAllowed.get()) {
            return Optional.empty();
        }

        final var tx = new Transaction();
        activeTxs.add(tx);
        return Optional.of(tx);
    }

    Transaction() {
        this.currentTxNum = TX_NUM.incrementAndGet();
    }

    public void pin(final Block block) {
        try (LogRecord logRecord = new TransactionStartLogRecord(TX_NUM.get())) {
            logRecord.save();
            blockBufferMap.put(block, bufferManager.pin(block));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void unpin(final Block block) {
        bufferManager.unpin(block);
        blockBufferMap.remove(block);
    }

    public void setInt(final Block block, final int val, final int off) {
        final Buffer buffer = blockBufferMap.get(block);

        if (buffer == null) {
            throw new RuntimeException("Buffer can not be reached without pinning");
        }

        concurrencyManager.getXLock(block);
        final int oldVal = buffer.getInt(off);

        final long lsn;

        try (SetIntLogRecord logRecord = new SetIntLogRecord(block, currentTxNum, off, oldVal, val)) {
            lsn = logRecord.save();
            buffer.setInt(val, off, currentTxNum, lsn);
        } catch (RuntimeException e) {
            // A clumsy way to manage rollback mechanism, first catch the exception then rollback then
            // throw the same exception again.
            recoveryManager.rollback();
            throw new RuntimeException(e);
        }

    }

    public void setStr(final Block block, final String val, final int off) {
        final Buffer buffer = blockBufferMap.get(block);

        if (buffer == null) {
            throw new RuntimeException("Buffer can not be reached without pinning");
        }

        concurrencyManager.getXLock(block);

        final long lsn;

        try (SetStringLogRecord logRecord = new SetStringLogRecord(block, currentTxNum, off, buffer.getString(off), val)) {
            lsn = logRecord.save();
            buffer.setString(val, off, currentTxNum, lsn);
        } catch (RuntimeException e) {
            // A clumsy way to manage rollback mechanism, first catch the exception then rollback then
            // throw the same exception again.
            recoveryManager.rollback();
            throw new RuntimeException(e);
        }


    }

    public void setBool(final Block block, final int off) {
        throw new RuntimeException("BOOL NOT SUPPORTED YET");
    }

    public int getInt(final Block block, final int off) {
        concurrencyManager.getSharedLock(block);
        final Buffer buffer = blockBufferMap.getOrDefault(block, bufferManager.pin(block));

        return buffer.getInt(off);
    }

    public String getStr(final Block block, final int off) {
        concurrencyManager.getSharedLock(block);
        final Buffer buffer = blockBufferMap.getOrDefault(block, bufferManager.pin(block));

        return buffer.getString(off);
    }

    public boolean getBool(final Block block) {
        return false;
    }


    public void commit() {
        recoveryManager.commit();
        blockBufferMap.keySet().forEach(block -> {
            blockBufferMap.get(block).flush(currentTxNum);
            bufferManager.unpin(block);
            concurrencyManager.xRelease(block);
            concurrencyManager.sRelease(block);
        });
        activeTxs.remove(this);
    }

    public void rollBack() {
        recoveryManager.doRollBack();
        blockBufferMap.keySet().forEach(bufferManager::unpin);
    }

    public long getCurrentTxNum() {
        return currentTxNum;
    }

    void flush() {
        bufferManager.flush(currentTxNum);
    }


}
