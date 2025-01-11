package org.berkerdb.db.record;

import java.util.Map;
import java.util.HashMap;
import java.util.function.Predicate;

public class TableInfo {

    private final Schema schema;
    private final String tableName;
    final Map<String, Integer> fieldNameToOffSetMap = new HashMap<>();


    final int fixedSlotSize;

    public String getTableName() {
        return tableName;
    }

    public TableInfo(final Schema schema, final String tableName) {
        this.schema = schema;
        this.tableName = tableName;

        // TO DO: Implement padding.
        this.fixedSlotSize = schema.getFieldSet()
                .stream()
                .filter(Predicate.not(Schema.Field::isVariableLength))
                .mapToInt(Schema.Field::size)
                .sum();

        int fixedCurrentOff = 0;
        int variableCurrentOff = fixedSlotSize;

        for (final var f : schema.getFieldSet()) {
            if (f.isVariableLength()) {
                fieldNameToOffSetMap.put(f.name(), variableCurrentOff);
                variableCurrentOff += Byte.BYTES;
                continue;
            }

            fixedCurrentOff += Integer.BYTES;
            fieldNameToOffSetMap.put(f.name(), fixedCurrentOff);
        }
    }

    public int calcFieldOff(final int slot, final String fieldName) {
        return (fixedSlotSize * slot) + fieldNameToOffSetMap.get(fieldName);
    }

    public int getFixedSlotSize() {
        return fixedSlotSize;
    }
    public Schema getSchema() {
        return schema;
    }
}
