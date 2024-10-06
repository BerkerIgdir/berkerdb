package org.berkerdb.db.record;

import java.util.HashMap;
import java.util.Map;

public class TableInfo {

    private final Schema schema;
    private final String tableName;
    final Map<String, Integer> fieldNameToOffSetMap = new HashMap<>();
    int slotSize;

    public String getTableName() {
        return tableName;
    }

    public TableInfo(final Schema schema, final String tableName) {
        this.schema = schema;
        this.tableName = tableName;
        slotSize = Integer.BYTES;
        for (final var f : schema.getFieldSet()) {
            slotSize += f.size();
            fieldNameToOffSetMap.put(f.name(), slotSize);
        }
    }

    public int calcFieldOff(final int slot, final String fieldName) {
        return (slotSize * slot) + fieldNameToOffSetMap.get(fieldName) + Integer.BYTES;
    }

    public Schema getSchema() {
        return schema;
    }
}
