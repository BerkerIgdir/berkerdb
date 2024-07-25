package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.buffer.Buffer;
import org.berkerdb.db.file.Block;
import org.berkerdb.db.log.*;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public class RecoveryManager {

    private final Transaction tx;
    private final LogManager logManager = Main.DB().getLogManager();

    public RecoveryManager(Transaction tx) {
        this.tx = tx;
    }

    public void setInt(final Buffer buffer, final int off, final int newVal) {
        final int oldVal = buffer.getInt(off);
        final Block block = buffer.getCurrentBlock();

        final long lsn;
        try (final var logRecord = new SetIntLogRecord(block, tx.getCurrentTxNum(), off, oldVal, newVal)) {
            lsn = logRecord.save();
        }

        buffer.setInt(newVal, off, tx.getCurrentTxNum(), lsn);
    }

    public void setString(final Buffer buffer, final int off, final String newVal) {
        final String oldVal = buffer.getString(off);
        final Block block = buffer.getCurrentBlock();

        final long lsn;

        try (final var logRecord = new SetStringLogRecord(block, tx.getCurrentTxNum(), off, oldVal, newVal)) {
            lsn = logRecord.save();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        buffer.setString(newVal, off, tx.getCurrentTxNum(), lsn);
    }

    public OptionalInt getInt(final Block block, final int off) {
        final List<Long> rollbackList = new ArrayList<>();
        final List<Long> commitList = new ArrayList<>();

        for (final var rec : logManager) {
            if (rec instanceof CommitLogRecord commitLogRecord) {
                commitList.add(commitLogRecord.getTxNum());
            }

            if (rec instanceof RollbackLogRecord rollbackLogRecord) {
                rollbackList.add(rollbackLogRecord.getTxNum());
            }

            if (rec instanceof SetIntLogRecord setIntLogRecord) {
                if (setIntLogRecord.getBlockNum() == block.blockNumber()) {
                    if (commitList.contains(setIntLogRecord.getTxNum()) &&
                            !rollbackList.contains(setIntLogRecord.getTxNum())) {
                        return OptionalInt.of(setIntLogRecord.getNewVal());
                    } else if (rollbackList.contains(setIntLogRecord.getTxNum())) {
                        return OptionalInt.of(setIntLogRecord.getOldVal());
                    }
                }
            }
        }

        return OptionalInt.empty();
    }

    public void doRollBack() {
        System.out.println(STR."Rollback for Transaction \{tx.currentTxNum}");
        for (LogRecord logRecord : logManager) {
            if (logRecord.getLogType() == LogRecord.LogType.START) {
                return;
            }

            if (logRecord.getTxNum() == tx.getCurrentTxNum()) {
                logRecord.undo(tx);
            }
        }
    }

    //For now it uses Quiescent Checkpoint logic.
    public void recovery() {
        final List<Long> finishedTransactions = new ArrayList<>();

        //TO DO: Refactor into a switch statement maybe?
        for (var logRecord : logManager) {
            if (logRecord == null) {
                continue;
            }
            if (logRecord.getLogType() == LogRecord.LogType.CHECKPOINT) {
                return;
            }
            if (logRecord.getLogType() == LogRecord.LogType.COMMIT ||
                    logRecord.getLogType() == LogRecord.LogType.ROLLBACK) {
                finishedTransactions.add(logRecord.getTxNum());
            } else if (logRecord.getLogType() == LogRecord.LogType.SET_INT ||
                    logRecord.getLogType() == LogRecord.LogType.SET_STRING) {
                if (!finishedTransactions.contains(logRecord.getTxNum())) {
                    logRecord.undo(tx);
                }
            }
        }

        try (CheckpointLogRecord logRecord = new CheckpointLogRecord(new long[]{tx.getCurrentTxNum()})) {
            final long lsn = logRecord.save();
            logManager.flush(lsn);
            tx.flush();
        }
    }

    public void commit() {
        try (CommitLogRecord logRecord = new CommitLogRecord(tx.getCurrentTxNum())) {
            final long lsn = logRecord.save();
            logManager.flush(lsn);
            tx.flush();
        }
    }

    public void rollback() {
        doRollBack();
        try (final RollbackLogRecord logRecord = new RollbackLogRecord(tx.getCurrentTxNum())) {
            final long lsn = logRecord.save();
            logManager.flush(lsn);
            tx.flush();
        }
    }
}
