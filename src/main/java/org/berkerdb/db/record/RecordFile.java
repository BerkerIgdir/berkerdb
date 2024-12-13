package org.berkerdb.db.record;


import org.berkerdb.db.file.Block;
import org.berkerdb.db.transaction.Transaction;

import static org.berkerdb.db.record.RecordPage.NOT_FOUND_SLOT;

public class RecordFile {

    private static final String TABLE_FILE_EXTENSION = ".tbl";

    public record RID(int blockNum, int slot) {
    }

    public RID getCurrentRID() {
        return this.currentRID;
    }

    private RecordPage rp;
    private RID currentRID;
    private final Transaction transaction;

    public RecordFile(final TableInfo tableInfo, final Transaction tx) {
        this.transaction = tx;

        if (tx.fileSize(tableInfo.getTableName()) == 0) {
            tx.append(tableInfo.getTableName());
            final Block block = new Block(tableInfo.getTableName(), 0);
            tx.pin(block);
            this.rp = new RecordPage(tableInfo, block, transaction);
            this.rp.format();
            this.currentRID = new RID(rp.getBlock().blockNumber(), 0);
            return;
        }

        this.rp = new RecordPage(tableInfo, new Block(tableInfo.getTableName(), 0), transaction);
        this.currentRID = new RID(rp.getBlock().blockNumber(), rp.findNextUsedSlot(0));
    }

    public int getInt(final String intFieldName) {
        return rp.getInt(currentRID.slot, intFieldName);
    }

    public String getStr(final String testStr) {
        return rp.getString(currentRID.slot, testStr);
    }

    public void setInt(final String intFieldName, int val) {
        rp.setInt(intFieldName, val, currentRID.slot);
    }

    public void setStr(final String strFieldName, final String val) {
        rp.setStr(strFieldName, val, currentRID.slot);
    }

    // Changes pointer to the next record in the block, if there is no next record in the block then a new block is created.
    public boolean next() {
        final int nextSlot = rp.findNextUsedSlot(currentRID.slot);

        if (nextSlot == NOT_FOUND_SLOT) {
            if (rp.isLastBlock()) {
                return false;
            }
            final Block block = new Block(rp.getLayout().getTableName(), rp.getBlock().blockNumber() + 1);
            changeCurrentRid(block);
            this.currentRID = new RID(currentRID.blockNum, nextSlot);
            return true;
        }

        this.currentRID = new RID(currentRID.blockNum, nextSlot);
        return true;
    }

    private void changeCurrentRid(final Block block) {
        rp.close();
        rp.setBlock(block);
        rp.getTransaction().pin(block);
    }

    public void beforeFirst() {
        final int firstSlot = rp.findNextUsedSlot(0);
        if (firstSlot == NOT_FOUND_SLOT) {
            this.currentRID = new RID(0, 0);
            return;
        }
        this.currentRID = new RID(currentRID.blockNum(), firstSlot);
    }

    public void moveToRID(final RID rid) {
        while (rid.slot != currentRID.slot && next()) {
        }
        this.currentRID = rid;
    }

    public void insert() {
        final int slot = rp.findNextAvailableSlot(0);
        if (slot == NOT_FOUND_SLOT) {
            // Check if it is the last block in the file.
            if (rp.isLastBlock()) {
                moveToANewBlock();
                return;
            }
            // Here we need to add a new block, thus a record page.
            moveToNextBlock();
            return;
        }
        rp.addRecord(slot);
    }

    public void delete() {
        this.rp.deleteRecord(currentRID.slot());
    }

    public void delete(final RID rid) {
        moveToRID(rid);
        this.rp.deleteRecord(currentRID.slot());
    }

    private void moveToNextBlock() {
        moveToBlock(currentRID.blockNum() + 1);
    }

    private void moveToBlock(final int blockNum) {
        rp.close();
        final String fileName = rp.getBlock().fileName();
        // TO DO: Refactor block num to long
        final Block block = new Block(fileName, blockNum);
        transaction.pin(block);
        rp.setBlock(block);
        rp.format();

        this.currentRID = new RID(block.blockNumber(), rp.findNextUsedSlot(0));
    }

    private void moveToANewBlock() {
        final long blockNum = transaction.append(rp.getBlock().fileName());
        // TO DO: Refactor blockNum to long in the future
        moveToBlock((int) blockNum);
    }

}
