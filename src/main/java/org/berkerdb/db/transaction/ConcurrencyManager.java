package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.file.Block;

public class ConcurrencyManager {

    private final LockTable lockTable;
    //Maybe deleted in the future.
    private final Transaction transaction;

    public ConcurrencyManager(Transaction transaction) {
        this.transaction = transaction;
        this.lockTable = Main.DB().getLockTable();
    }

    public void getSharedLock(final Block block) {
        lockTable.getSharedLock(block);
    }

    public void getXLock(final Block block) {
         lockTable.getXLock(block);
    }

    public void xRelease(final Block block){
        lockTable.xRelease(block);
    }
    public void sRelease(final Block block){
        lockTable.sRelease(block);
    }
    public Transaction getTransaction() {
        return transaction;
    }
}
