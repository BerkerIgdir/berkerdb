package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;

import java.util.*;

public class RepeatableReadTransaction extends Transaction {

    record RepeatableReadValRecord<T>(Block block, int off, T val) {
    }

    final Map<Block, RepeatableReadValRecord<?>> blockToValMap = new HashMap<>();

    @Override
    public int getInt(final Block block, final int off) {
        return Optional.ofNullable(blockToValMap.get(block))
                .filter(repeatableReadValRecord -> repeatableReadValRecord.off == off)
                .map(RepeatableReadValRecord::val)
                .map(Integer.class::cast)
                .orElseGet(() -> {
                    try {
                        final var val = super.getInt(block, off);
                        concurrencyManager.sRelease(block);
                        blockToValMap.put(block, new RepeatableReadValRecord<>(block, off, val));
                        return val;
                    } catch (RuntimeException e) {
                        final var val = recoveryManager.getInt(block, off).orElse(Integer.MIN_VALUE);
                        blockToValMap.put(block, new RepeatableReadValRecord<>(block, off, val));
                        return val;
                    }
                });
    }

    @Override
    public String getStr(final Block block, final int off) {
        return Optional.ofNullable(blockToValMap.get(block))
                .filter(repeatableReadValRecord -> repeatableReadValRecord.off == off)
                .map(RepeatableReadValRecord::val)
                .map(String.class::cast)
                .orElseGet(() -> {
                    try {
                        final var val = super.getStr(block, off);
                        concurrencyManager.sRelease(block);
                        blockToValMap.put(block, new RepeatableReadValRecord<>(block, off, val));
                        return val;
                    } catch (RuntimeException e) {
                        final var val = recoveryManager.getString(block, off).orElse(null);
                        blockToValMap.put(block, new RepeatableReadValRecord<>(block, off, val));
                        return val;
                    }
                });
    }

    @Override
    public boolean getBool(final Block block) {
        final var val = super.getBool(block);
        concurrencyManager.sRelease(block);
        return val;
    }

}
