package org.berkerdb.db.log;

import org.berkerdb.Main;
import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.io.Closeable;

//TO DO: All log record classes will be refactored through
// an abstract class to reduce the amount of repetitive code they have right now.
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

        public static LogType fromInt(final int type) {
            return switch (type) {
                case 0 -> START;
                case 1 -> SET_INT;
                case 2 -> SET_STRING;
                case 3 -> COMMIT;
                case 4 -> ROLLBACK;
                case 5 -> CHECKPOINT;
                default -> throw new IllegalArgumentException("Unrecognized enum type");
            };
        }

        public int getNumber() {
            return number;
        }

        LogType(int number) {
            this.number = number;
        }
    }

    LogType getLogType();

    long save();

    void undo(final Transaction tx);

    long getTxNum();
}
