package org.berkerdb.db.record;

import java.util.HashSet;
import java.util.Set;
import java.sql.Types;

public class Schema {
    public record Field(String name, int size, int type) {
    }

    private final Set<Field> fieldSet = new HashSet<>();

    public void addInt(final String fieldName) {
        fieldSet.add(new Field(fieldName, Integer.BYTES, Types.INTEGER));
    }

    public void addVarChar(final String fieldName, final int length) {
        fieldSet.add(new Field(fieldName, Integer.BYTES + (length * Byte.BYTES), Types.VARCHAR));
    }

    public Set<Field> getFieldSet() {
        return fieldSet;
    }
}
