package org.berkerdb.db.log;

import org.berkerdb.Main;
import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.io.Closeable;

public interface LogRecord extends Closeable {
    LogManager logManager = Main.DB().getLogManager();

    enum LogType {
        START(0),
        SET_INT(1),
        SET_STRING(2),
        COMMIT(3),
        ROLLBACK(4),
        CHECKPOINT(5);

        private final int number;

        LogType(int number) {
            this.number = number;
        }
    }

    default <T> long save(final Block block, final long tx, final T oldV, final T newV){
        return 0L;
    }

    LogType getLogType();

    long save();

    void undo(final Transaction tx);
}
