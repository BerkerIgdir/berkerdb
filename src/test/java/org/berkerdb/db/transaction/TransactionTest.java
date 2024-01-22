package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;

public class TransactionTest {

    public void fundamentalBehaviourTest() {
        final Transaction underTest = new Transaction();
        final Block block = new Block("test", 0);
        final int testVal = 123;
        underTest.pin(block);
        underTest.setInt(block, testVal, 0);

    }

}
