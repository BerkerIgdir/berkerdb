package org.berkerdb.db.transaction;

import org.berkerdb.Main;
import org.berkerdb.db.file.Block;
import org.berkerdb.db.file.LocalMemoryPage;
import org.berkerdb.db.log.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ReadOnlyTransaction extends Transaction {
    private final LogManager logManager = Main.DB().getLogManager();
    protected final Map<Block, LocalMemoryPage> blockPageMap = new HashMap<>();

    public void pin(final Block block) {
        final var page = new LocalMemoryPage();
        page.read(block);

        for (final var logRec : logManager) {
            if (logRec.getTxNum() == getCurrentTxNum()) {
                continue;
            }

            // We need a timestamp check here to make it sure that the committed transaction did not begin after this tx.
            if ((logRec.getLogType() == LogRecord.LogType.COMMIT ||
                    logRec.getLogType() == LogRecord.LogType.ROLLBACK ||
                    logRec.getLogType() == LogRecord.LogType.CHECKPOINT) &&
                    logRec.getTxNum() < getCurrentTxNum()) {
                break;
            }

            if (logRec instanceof SetStringLogRecord stringLog && stringLog.getBlockNum() == block.blockNumber()) {
                page.setByteArrayToMemory(stringLog.getOff(), stringLog.getOldVal().getBytes());
            }

            if (logRec instanceof SetIntLogRecord intLog && intLog.getBlockNum() == block.blockNumber()) {
                page.setIntToMemory(intLog.getOff(), intLog.getOldVal());
            }

        }
        //No transaction needed so the record type log also must be changed.
        try (LogRecord logRecord = new TransactionStartLogRecord(TX_NUM.get())) {
            logRecord.save();
            blockPageMap.put(block, page);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void setInt(final Block block, final int val, final int off) {
        throw new RuntimeException("Set operations are not allowed in read only transactions!");
    }

    @Override
    public void setStr(final Block block, final String val, final int off) {
        throw new RuntimeException("Set operations are not allowed in read only transactions!");
    }

    @Override
    public void setBool(final Block block, int off) {
        throw new RuntimeException("Set operations are not allowed in read only transactions!");
    }

    @Override
    public int getInt(final Block block, int off) {
        return Optional.ofNullable(blockPageMap.get(block))
                .map(p -> p.getIntFromMemory(off))
                .orElseThrow(() -> new RuntimeException("A block can not be read without pinning!"));
    }

    @Override
    public void commit() {
        blockPageMap.values().forEach(LocalMemoryPage::close);
    }

    @Override
    public String getStr(final Block block, int off) {
        return Optional.ofNullable(blockPageMap.get(block))
                .map(p -> p.getStringFromMemory(off))
                .orElseThrow(() -> new RuntimeException("A block can not be read without pinning!"));
    }
}
