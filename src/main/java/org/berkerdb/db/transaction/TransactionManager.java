package org.berkerdb.db.transaction;

public class TransactionManager {
    public enum SupportedTxType {
        STANDARD, READ_COMMITTED, REPEATABLE_READ, READ_ONLY;
    }

    public static Transaction newTx(final SupportedTxType supportedTxType) {
        return switch (supportedTxType) {
            case STANDARD -> new Transaction();
            case READ_COMMITTED -> new ReadCommittedTransaction();
            case REPEATABLE_READ -> new RepeatableReadTransaction();
            case READ_ONLY -> new ReadOnlyTransaction();
        };
    }
}
