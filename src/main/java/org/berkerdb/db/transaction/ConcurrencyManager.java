package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.file.Block;

import java.util.Set;
import java.util.HashSet;


public class ConcurrencyManager {

    static int dem = 0;
    private final LockTable lockTable;

    //Maybe deleted in the future.
    private final Transaction transaction;

    private final Set<Block> xLockBLocks = new HashSet<>();
    private final Set<Block> sLockBLocks = new HashSet<>();

    public ConcurrencyManager(Transaction transaction) {
        this.transaction = transaction;
        this.lockTable = Main.DB().getLockTable();
    }

    public void getSharedLock(final Block block) {
        if (sLockBLocks.contains(block) || xLockBLocks.contains(block)) {
            return;
        }
        lockTable.getSharedLock(block);
        sLockBLocks.add(block);
    }

    public void getXLock(final Block block) {
        if (xLockBLocks.contains(block)) {
            return;
        }

        lockTable.getXLock(block);
        xLockBLocks.add(block);
        dem++;
    }

    public void xRelease(final Block block) {
        lockTable.xRelease(block);
        xLockBLocks.remove(block);
        dem--;
    }

    public void sRelease(final Block block) {
        lockTable.sRelease(block);
        sLockBLocks.remove(block);
    }

    public Transaction getTransaction() {
        return transaction;
    }
}
