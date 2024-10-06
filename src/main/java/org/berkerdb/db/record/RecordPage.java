package org.berkerdb.db.record;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.sql.Types;
import java.util.Map;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;

public class RecordPage {

    static final int AVAILABLE_SLOT = 1;
    static final int UNAVAILABLE_SLOT = 0;
    static final int NOT_FOUND_SLOT = -1;

    private final Transaction transaction;
    private final TableInfo layout;

    private Block block;

    final int MAX_SLOT_NUM;

    public RecordPage(final TableInfo tableInfo, final Block block, final Transaction tx) {
        this.layout = tableInfo;
        this.block = block;
        this.transaction = tx;
        this.MAX_SLOT_NUM = (BLOCK_SIZE / tableInfo.slotSize) - 1;
    }

    public void setString(final int slot, final String fieldName, final String val) {
        final int pos = layout.calcFieldOff(slot, fieldName);
        transaction.setStr(block, val, pos);
    }

    public String getString(final int slot, final String fieldName) {
        final int pos = layout.calcFieldOff(slot, fieldName);
        return transaction.getStr(block, pos);
    }

    public int getInt(final int slot, final String fieldName) {
        final int pos = layout.calcFieldOff(slot, fieldName);
        return transaction.getInt(block, pos);
    }

    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            final int tempSlot = slot;
            transaction.setInt(block, AVAILABLE_SLOT, getSlotBeginOff(slot));
            layout.getSchema().getFieldSet().forEach(f -> {
                if (f.type() == Types.INTEGER) {
                    transaction.setInt(block, 0, layout.calcFieldOff(tempSlot, f.name()));
                }

                if (f.type() == Types.VARCHAR) {
                    transaction.setStr(block, "", layout.calcFieldOff(tempSlot, f.name()));
                }

                if (f.type() == Types.BOOLEAN) {
                    // TO DO : Implement BOOL Support;

                }
            });
            slot++;
        }
    }

    public void addRecord(final int slot) {
        final int off = getSlotBeginOff(slot);
        transaction.setInt(block, UNAVAILABLE_SLOT, off);
    }

    public void setInt(final String fieldName, final int val, final int slot) {
        transaction.setInt(block, val, getLayout().calcFieldOff(slot, fieldName));
    }

    public void setStr(final String fieldName, final String val, final int slot) {
        transaction.setStr(block, val, getLayout().calcFieldOff(slot, fieldName));
    }

    public void addRecordAfter(final int slot, final Map<String, Object> fieldNameValMap) {
    }

    public void modifyRecord(final long slot, final Map<String, Object> fieldNameValMap) {
    }

    public void deleteRecord(final int slot) {
        transaction.setInt(block, AVAILABLE_SLOT, getSlotBeginOff(slot));
    }

    public int findNextAvailableSlot(final int slot) {
        return findNextSlot(slot, AVAILABLE_SLOT);
    }

    public int findNextUsedSlot(final int slot) {
        return findNextSlot(slot, UNAVAILABLE_SLOT);
    }

    private int findNextSlot(final int slot, final int availability) {
        int slotNum = slot;
        while (isValidSlot(slotNum)) {
            if (transaction.getInt(block, getSlotBeginOff(slotNum)) == availability) {
                return slotNum;
            }
            slotNum++;
        }
        return NOT_FOUND_SLOT;
    }


    public boolean isLastBlock() {
        return (transaction.fileSize(block.fileName()) / BLOCK_SIZE) >= block.blockNumber();
    }


    public Transaction getTransaction() {
        return transaction;
    }

    public TableInfo getLayout() {
        return layout;
    }

    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    private int getSlotBeginOff(int slot) {
        return layout.slotSize * slot;
    }

    private boolean isValidSlot(final int slot) {
        return slot < MAX_SLOT_NUM;
    }

    public void close() {
        transaction.unpin(block);
    }
}
