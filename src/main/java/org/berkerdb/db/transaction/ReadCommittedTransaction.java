package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;

public class ReadCommittedTransaction extends Transaction {

    @Override
    public int getInt(final Block block, final int off) {
        try {
            final var val = super.getInt(block, off);
            concurrencyManager.sRelease(block);
            return val;
        } catch (RuntimeException e) {
            return recoveryManager.getInt(block, off).orElse(Integer.MIN_VALUE);
        }
    }

    @Override
    public String getStr(final Block block, final int off) {
        try {
            final var val = super.getStr(block, off);
            concurrencyManager.sRelease(block);
            return val;
        } catch (RuntimeException e) {
            // TO DO: Refactor it to become more consistent with a optional based api.
            // Returning null from an optional based api is not a good look.
            return recoveryManager.getString(block, off).orElse(null);
        }

    }

    @Override
    public boolean getBool(final Block block) {
        final var val = super.getBool(block);
        concurrencyManager.sRelease(block);
        return val;
    }
}
