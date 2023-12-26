package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;
import org.berkerdb.db.log.SetStringLogRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TransactionTest {


    @Test
    public void basicMemoryTest() {

        Runtime runtime = Runtime.getRuntime();
//        long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
//        System.out.println("Used Memory before " + usedMemoryBefore);
//        System.out.println("Total amount of cores " + runtime.availableProcessors());

        final int recordCount = 200;
        final Block block = new Block("junk", 0);
        long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
        for (int i = 0; i < recordCount; i++) {
            try (var record = new SetStringLogRecord(block, i, 0, "aa", "a")) {
                long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();

                System.out.println("Used Memory before " + ( usedMemoryAfter - usedMemoryBefore));
                record.save();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


//        System.out.println("Memory increased: " + (usedMemoryAfter - usedMemoryBefore));
    }
}
