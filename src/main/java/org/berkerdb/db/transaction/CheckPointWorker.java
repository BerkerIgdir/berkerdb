package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.log.CheckpointLogRecord;
import org.berkerdb.db.log.LogManager;

import static org.berkerdb.db.transaction.Transaction.activeTxs;
import static org.berkerdb.db.transaction.Transaction.isTxAllowed;

public class CheckPointWorker {
    private final LogManager logManager = Main.DB().getLogManager();
    static final int DEFAULT_TX_LIST_SIZE = 100;

    void doWork() {
        isTxAllowed.compareAndSet(true, false);

//        long[] txNameArr = new long[DEFAULT_TX_LIST_SIZE];
//        int i = 0;
//
//        for (var record : logManager) {
//            if (record.getLogType() == LogRecord.LogType.COMMIT ||
//                    record.getLogType() == LogRecord.LogType.ROLLBACK ||
//                    record.getLogType() == LogRecord.LogType.CHECKPOINT ||
//                    record.getTxNum() == 0) {
//                break;
//            }
//            if (i == DEFAULT_TX_LIST_SIZE - 1) {
//                txNameArr = resizeTheArray(txNameArr);
//            }
//            txNameArr[i] = record.getTxNum();
//            i++;
//        }

        final long[] txArray = activeTxs.stream().map(Transaction::getCurrentTxNum).mapToLong(Long::longValue).toArray();
        try (final var checkTx = new CheckpointLogRecord(txArray)) {
            checkTx.save();
            activeTxs.forEach(Transaction::flush);
            isTxAllowed.set(true);
        }
    }


//    private long[] resizeTheArray(long[] txNameArr) {
//        final var temp = new long[txNameArr.length * 2];
//        System.arraycopy(txNameArr, 0, temp, 0, txNameArr.length);
//        return temp;
//    }

}
