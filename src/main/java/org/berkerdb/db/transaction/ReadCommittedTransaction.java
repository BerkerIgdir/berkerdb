package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;

public class ReadCommittedTransaction extends Transaction {

    @Override
    public int getInt(Block block, int off) {
        try {
            final var val = super.getInt(block, off);
            concurrencyManager.sRelease(block);
            return val;
        } catch (RuntimeException e) {
            return recoveryManager.getInt(block,off).orElse(Integer.MIN_VALUE);
        }
    }

    @Override
    public String getStr(Block block, int off) {
        final var val = super.getStr(block, off);
        concurrencyManager.sRelease(block);
        return val;
    }

    @Override
    public boolean getBool(Block block) {
        final var val = super.getBool(block);
        concurrencyManager.sRelease(block);
        return val;
    }
}
