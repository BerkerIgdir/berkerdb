package org.berkerdb.db.record;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Set;

public class VariableRecordPage {

    static final int VARIABLE_RECORD_START_OFFSET_OFF = 0;
    static final int LOCATION_ARRAY_SIZE_OFFSET = Integer.BYTES;
    static final int LOCATION_ARRAY_BEGIN_OFF = 2 * Integer.BYTES;

    private final TableInfo layout;
    private final Transaction transaction;

    private Block block;

    public VariableRecordPage(final TableInfo tableInfo, final Block block, final Transaction tx) {
        this.layout = tableInfo;
        this.block = block;
        this.transaction = tx;
    }

    public void insert() {
        if (isBlockFull()) {
            throw new RuntimeException(String.format("The block %d is full", block.blockNumber()));
        }

        final int lastRecOff = getSlotOff(getRecordCount());
        final int recordSize = this.layout.getSchema().getTotalSize();
        final Set<Schema.Field> fieldSet = this.layout.getSchema().getFieldSet();
        int remainingOffset = recordSize;
        for (final var field : fieldSet) {
            if (field.type() == Types.INTEGER) {
                transaction.setInt(block, 0, lastRecOff - remainingOffset);
                remainingOffset -= field.size();
                continue;
            }
            // We do not check if it is variable because we already know that the variable fields will come after
            // than the fixed fields due to fact that Field record class implements the comparable interface.
            if (field.type() == Types.VARCHAR) {
                transaction.setStr(block, "", lastRecOff - remainingOffset);
                remainingOffset -= field.size();
            }
        }
    }


    public String getVariableString(final int slot, final String fieldName) {
        final int getSlotOff = getSlotOff(slot);
        final int fieldCardinality = layout.getSchema().getCardinalityOfField(fieldName);
        int variableOff = getSlotOff + layout.getFixedSlotSize();
        for (int i = 0; i < fieldCardinality; i++) {
            variableOff += transaction.getStr(block, variableOff).length();
        }

        return transaction.getStr(block, variableOff);
    }


    public int getInt(final int slot, final String fieldName) {
        return transaction.getInt(block, getFixedFieldOff(fieldName, slot));
    }

    public void setInt(final String fieldName, final int val, final int slot) {
        transaction.setInt(block, val, getFixedFieldOff(fieldName, slot));
    }

    public void setStr(final String fieldName, final String val, final int slot) {
        transaction.setStr(block, val, getFixedFieldOff(fieldName, slot));
    }

    public void delete(final int slot) {
        final int recordOff = getSlotOff(slot);
        final int recordOffBefore = getSlotOff(slot - 1);
        final int currentRecordLength = recordOffBefore - recordOff;
        final int recordCount = getRecordCount();
        final int lastRecordOff = getSlotOff(recordCount);
        final int arraySlotOff = getArrayElementOff(slot);
        final int arraySlotOffAfter = getArrayElementOff(slot + 1);

        final ByteBuffer buffer = transaction.getContentOfBuffer(block);

        // Delete the slot and move the records after it.
        buffer.put(lastRecordOff + currentRecordLength, buffer, lastRecordOff, currentRecordLength);
        // Modify the slot array accordingly
        buffer.put(arraySlotOff, buffer, arraySlotOffAfter, (recordCount - slot) * Integer.BYTES);

        // Set it in transaction.
        transaction.setContentOfBuffer(block, buffer);
    }

    public void next() {
        // No need to use next since we already keep all the offsets at the beginning of the block.
    }

    public void format() {
        // Spanned Record Marker
        transaction.setInt(block, 0, 0);
        // Size of record location array
        transaction.setInt(block, 0, Integer.BYTES);
    }

    private boolean isBlockFull() {
        final int numberOfRecs = getRecordCount();
        final int lastArrayEntryOff = getArrayElementOff(numberOfRecs);
        final int locationOfLastRecord = transaction.getInt(block, lastArrayEntryOff);

        return locationOfLastRecord == lastArrayEntryOff + Integer.BYTES;
    }


    private int getSlotOff(final int slot) {
        final int arrayElementOff = getArrayElementOff(slot);
        return transaction.getInt(block, arrayElementOff);
    }

    private int getArrayElementOff(final int slot) {
        return LOCATION_ARRAY_BEGIN_OFF + (slot * Integer.BYTES);
    }

    private int getFixedFieldOff(final String fieldName, final int slot) {
        final int getSlotOff = getSlotOff(slot);
        final int pos = layout.getSchema().getCardinalityOfField(fieldName) * Integer.BYTES;

        return getSlotOff + pos;
    }

    private int getRecordCount() {
        return transaction.getInt(block, LOCATION_ARRAY_SIZE_OFFSET);
    }
}
