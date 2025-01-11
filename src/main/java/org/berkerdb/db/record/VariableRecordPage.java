package org.berkerdb.db.record;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.List;
import java.util.Set;

// This part turned out to be more complex than I thought, thus for now we are going to continue with fixed size records.
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
        if (isBlockFull(this.block)) {
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
        final int variableOff = getVariableOff(slot, fieldName);

        return transaction.getStr(block, variableOff);
    }

    public int getInt(final int slot, final String fieldName) {
        return transaction.getInt(block, getFixedFieldOff(fieldName, slot));
    }

    public void setInt(final String fieldName, final int val, final int slot) {
        transaction.setInt(block, val, getFixedFieldOff(fieldName, slot));
    }

    public void setStr(final String fieldName, final String val, final int slot) {
        final int arrayOff = getArrayElementOff(slot);
        final int recordCount = getRecordCount();
        final int lastRecordOff = getSlotOff(recordCount);

        // Check if this slot exists or this is the last record in the block.
        if (arrayOff == -1) {
            throw new IllegalArgumentException("The slot you are looking for does not exist");
        }

        final int variableOff = getVariableOff(slot, fieldName);

        // We do know the variable off at this point, here we need to decide if the new length of the string fits in current block.
        // if it fits just move the entries after it, modify the slot size and slot array accordingly.
        // if it does not fit in the current block, first check if there is enough space in the next block,
        // if there is then move the last entry in this block into next one partially,
        // if there is no space in next block to repeat the process for the block next to next block until you find enough space.
        final int oldStrSize = transaction.getStr(block, variableOff).length();
        final int newStrSize = val.length();

        final ByteBuffer buffer = transaction.getContentOfBuffer(block);
        if (newStrSize <= oldStrSize) {
            // There is no space problem then we can simply move everything to right.
            final int lengthDiff = oldStrSize - newStrSize;
            buffer.put(lastRecordOff + lengthDiff, buffer, lastRecordOff, variableOff - lastRecordOff);
            transaction.setStr(block, val, variableOff + lengthDiff);
            return;
        } else {
            // We do have to check if it fits the current block.
            final int lengthDiff = newStrSize - oldStrSize;
            if (isBlockFull(this.block, lengthDiff)) {
                // if there is not enough place in current block we need to check consecutive block.
                // if there is no space in consecutive block we need to check the block next it to it until we find enough space first,
                // then we start pushing records/values from the latest non-available block to available one until we place our new string in out current block.
                Block localBlock = new Block(block.fileName(), this.block.blockNumber() + 1);
                while (isBlockFull(localBlock, lengthDiff)) {
                    localBlock = new Block(block.fileName(), localBlock.blockNumber() + 1);
                    transaction.pin(localBlock);
                }
                final int loopCount = localBlock.blockNumber() - this.block.blockNumber();

                for (int i = 0; i < loopCount ; i++) {
                    // TO DO: Move logic comes here.
                    // Our moving granularity is variable field, which means we do not move fields arbitrarily but only variable fields
                    // If we need to move a fixed variable due to space constraints then we do move the whole record to next block.
                    // Step 1: Detect the field or fields to get moved in the current block.
                    // Step 2: Move it to the next block and modify the next block accordingly.
                    // Step 3: Repeat it until the necessary fields/records are moved.
                    final int lastEntryOff = getLastEntryOff();

                }

//                transaction.pin(localBlock);
            }
        }

        transaction.setStr(block, val, variableOff);
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

        // Check if this slot exists or this is the last record in the block.
        if (arraySlotOff == -1) {
            throw new IllegalArgumentException("The slot you are looking for does not exist");
        }

        if (isLastElement(slot)) {
            // There is no need to move any entry, all we need to do is to change the array offset and array slots.
            buffer.put(arraySlotOff, buffer, arraySlotOffAfter, (recordCount - slot) * Integer.BYTES);
            // Also decrement the record count by one
            buffer.putInt(LOCATION_ARRAY_SIZE_OFFSET, recordCount - 1);

            // Set it in transaction.
            transaction.setContentOfBuffer(block, buffer);
            return;
        }

        // Delete the slot and move the records after it.
        buffer.put(lastRecordOff + currentRecordLength, buffer, lastRecordOff, recordOff - lastRecordOff);
        // Modify the slot array accordingly
        buffer.put(arraySlotOff, buffer, arraySlotOffAfter, (recordCount - slot) * Integer.BYTES);
        // Also decrement the record count by one
        buffer.putInt(LOCATION_ARRAY_SIZE_OFFSET, recordCount - 1);

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

    private boolean isBlockFull(final Block block) {
        return isBlockFull(block, 0);
    }

    private boolean isBlockFull(final Block block, final int neededSpace) {
        final int lastArrayEntryOff = getLastArrayEntryOff();
        final int locationOfLastRecord = transaction.getInt(block, lastArrayEntryOff);

        return locationOfLastRecord == (lastArrayEntryOff + Integer.BYTES + neededSpace);
    }

    private int getLastArrayEntryOff() {
        final int numberOfRecs = getRecordCount();
        return getArrayElementOff(numberOfRecs);
    }

    private int getLastEntryOff() {
        final int lastArraySlotOff = getLastArrayEntryOff();
        return transaction.getInt(block,lastArraySlotOff);
    }
    private int getSlotOff(final int slot) {
        final int arrayElementOff = getArrayElementOff(slot);
        return transaction.getInt(block, arrayElementOff);
    }

    private int getOffsetToMove(final int slot, final int neededSpace) {
        final int getSlotOff = getSlotOff(slot);
        final int fieldCardinality = layout.getSchema().getFieldSet().size();
        int variableOff = getSlotOff + layout.getFixedSlotSize();

        int strLen;
        for (int i = 0; i < fieldCardinality; i++) {
            strLen = transaction.getStr(block, variableOff).length();
            variableOff += strLen;
            if (strLen >= neededSpace){
                break;
            }
        }
        if (layout.getFixedSlotSize() -)
        return variableOff;
    }

    private int getVariableOff(final int slot, final String fieldName) {
        final int getSlotOff = getSlotOff(slot);
        final int fieldCardinality = layout.getSchema().getCardinalityOfField(fieldName);
        int variableOff = getSlotOff + layout.getFixedSlotSize();
        for (int i = 0; i < fieldCardinality; i++) {
            variableOff += transaction.getStr(block, variableOff).length();
        }
        return variableOff;
    }

    private int getArrayElementOff(final int slot) {
        final int recCount = getRecordCount();

        // Since slot numeration is array based it begins from 0, but record count is not. When we compare these two
        // We have to take this into consideration.
        if ((recCount - 1) < slot) {
            return -1;
        }
        return LOCATION_ARRAY_BEGIN_OFF + (slot * Integer.BYTES);
    }

    private boolean isLastElement(final int slot) {
        return (getRecordCount() - 1) == slot;
    }

    private int getFixedFieldOff(final String fieldName, final int slot) {
        final int getSlotOff = getSlotOff(slot);
        final int pos = layout.getSchema().getCardinalityOfField(fieldName) * Integer.BYTES;

        return getSlotOff + pos;
    }

    private int getRecordCount() {
        return transaction.getInt(block, LOCATION_ARRAY_SIZE_OFFSET);
    }

    private void setRecordCount(final int val) {
        transaction.setInt(block, val, LOCATION_ARRAY_SIZE_OFFSET);
    }

}
