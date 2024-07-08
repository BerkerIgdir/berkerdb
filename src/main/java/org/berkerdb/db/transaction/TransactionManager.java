package org.berkerdb.db.transaction;

import org.berkerdb.db.file.Block;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {

    private final Map<Transaction, Block> transactionToBlockMap = new ConcurrentHashMap<>();

}
