package org.berkerdb.db.record;

import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.sql.Types;

public class Schema {
    public record Field(String name, int size, int type, boolean isVariableLength) implements Comparable<Field> {
        public Field {
            if (isVariableLength) {
                type = Types.VARCHAR;
                size = Byte.BYTES;
            }
        }

        @Override
        public int compareTo(@NotNull Field o) {
            if (this.isVariableLength && !o.isVariableLength) {
                return 1;
            } else {
                return this.name.compareTo(o.name);
            }
        }
    }

    private int totalSize = 0;
    private final Set<Field> fieldSet = new TreeSet<>();
    private final Set<String> fieldNameSet = new HashSet<>();

    public void addInt(final String fieldName) {
        checkFieldName(fieldName);

        fieldSet.add(new Field(fieldName, Integer.BYTES, Types.INTEGER, false));
        totalSize += Integer.BYTES;
    }


    public void addVarChar(final String fieldName, final int length) {
        checkFieldName(fieldName);

        int fieldSize = length * Byte.BYTES;
        fieldSet.add(new Field(fieldName, fieldSize, Types.VARCHAR, false));
        totalSize += fieldSize;
    }

    public void addVarChar(final String fieldName) {
        checkFieldName(fieldName);

        fieldSet.add(new Field(fieldName, Byte.BYTES, Types.VARCHAR, true));
        totalSize += Byte.BYTES;
    }

    public int getCardinalityOfField(final String fieldName) {
        int cardinality = 1;
        for (final var field : fieldSet) {
            if (field.name.equalsIgnoreCase(fieldName)) {
                break;
            }
            cardinality++;
        }
        return cardinality;
    }

    public Set<Field> getFieldSet() {
        return fieldSet;
    }

    public int getTotalSize() {
        return totalSize;
    }

    private void checkFieldName(String fieldName) {
        if (fieldNameSet.contains(fieldName.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Field name already exists!");
        }
        fieldNameSet.add(fieldName.toLowerCase());
    }
}
