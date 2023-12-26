package org.berkerdb.db.log;

import org.berkerdb.db.file.Block;

public record BasicValRecord(String fileName, int blockNum, long tx, int oldValOff) {
}
