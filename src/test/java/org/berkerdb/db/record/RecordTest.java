package org.berkerdb.db.record;


import org.berkerdb.db.transaction.TransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class RecordTest {
    static final String TEST_TABLE = "testTable";

    @AfterEach
    void cleanAfter() throws IOException {
        //Delete the temp files.
        final String homeDirectory = System.getProperty("user.home");
        final Path homePath = Paths.get(homeDirectory);
        final Path dirPath = homePath.resolve("test");
        Files.deleteIfExists(dirPath.resolve(TEST_TABLE));
        Files.deleteIfExists(dirPath.resolve("log_file"));
        Files.deleteIfExists(dirPath);
    }

    @Test
    public void fundBehaviourTest() {
        final int testVal = 999;

        final var schema = new Schema();
        final String intFieldName = "testInt";
        final String testStr = "testStr";

        schema.addInt(intFieldName);
        schema.addVarChar(testStr, 10);

        final var tableInfo = new TableInfo(schema, TEST_TABLE);
        final var tx = TransactionManager.newTx(TransactionManager.SupportedTxType.STANDARD);
        final var file = new RecordFile(tableInfo, tx);
        file.beforeFirst();
        file.insert();

        final RecordFile.RID rid = file.getCurrentRID();

        assertEquals(0, file.getInt(intFieldName));
        assertEquals("", file.getStr(testStr));

        file.setInt(intFieldName, testVal);
        file.setStr(testStr, testStr);

        assertEquals(file.getInt(intFieldName), testVal);
        assertEquals(file.getStr(testStr), testStr);

        final boolean isNext = file.next();
        assertTrue(isNext);

        final RecordFile.RID currentRID = file.getCurrentRID();
        assertEquals(rid, currentRID);

        file.delete();
        file.beforeFirst();

        assertFalse(file.next());
        tx.commit();
    }
    @Test
    public void variableRecordTest() {
        final int testVal = 999;

        final var schema = new Schema();
        final String intFieldName = "testInt";
        final String testStr = "testStr";

        schema.addInt(intFieldName);
        schema.addVarChar(testStr);

        final var tableInfo = new TableInfo(schema, TEST_TABLE);
        final var tx = TransactionManager.newTx(TransactionManager.SupportedTxType.STANDARD);
        final var file = new RecordFile(tableInfo, tx);
        file.beforeFirst();
        file.insert();

        final RecordFile.RID rid = file.getCurrentRID();

        assertEquals(0, file.getInt(intFieldName));
        assertEquals("", file.getStr(testStr));

        file.setInt(intFieldName, testVal);
        file.setStr(testStr, testStr);

        assertEquals(file.getInt(intFieldName), testVal);
        assertEquals(file.getStr(testStr), testStr);

        final boolean isNext = file.next();
        assertTrue(isNext);

        final RecordFile.RID currentRID = file.getCurrentRID();
        assertEquals(rid, currentRID);

        file.delete();
        file.beforeFirst();

        assertFalse(file.next());
        tx.commit();
    }

}
