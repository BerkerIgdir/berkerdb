package org.berkerdb.db.log;

import java.lang.foreign.MemoryLayout;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

public class LogRecordMemoryLayout {


    static final String RECORD_TYPE = "RECORD_TYPE";
    static final String BLOCK_NUMBER = "BLOCK_NUMBER";
    static final String OFFSET = "OLD_VAL_OFFSET";
    static final String TX_NUM = "TX_NUM";
    static final String FILENAME_LENGTH = "FILENAME_LENGTH";
    static final String FILENAME = "FILENAME";
    static final String NEW_VAL = "NEW_VAL";
    static final String NEW_VAL_LENGTH = "NEW_VAL_LENGTH";
    static final String OLD_VAL_LENGTH = "OLD_VAL_LENGTH";
    static final String OLD_VAL = "OLD_VAL";


    static final int LOG_TYPE_OFF = 0;
    static final int BLOCK_NUM_OFF = 4;
    static final int FILENAME_LENGTH_OFF = 8;
    static final int OFF_OFFSET = 12;
    static final int TX_NUM_OFF = 16;
    static final int OLD_VAL_OFF = 24;

    //String Record
    static final int OLD_VAL_LENGTH_OFF = 24;
    static final int NEW_VAL_LENGTH_OFF = 28;

    static final MemoryLayout BASIC_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_INT.withName(RECORD_TYPE),
            ValueLayout.JAVA_INT.withName(BLOCK_NUMBER),
            ValueLayout.JAVA_INT.withName(FILENAME_LENGTH),
            ValueLayout.JAVA_INT.withName(OFFSET),
            ValueLayout.JAVA_LONG.withName(TX_NUM)
    );
    static final VarHandle RECORD_TYPE_HANDLE = BASIC_LAYOUT.varHandle(groupElement(RECORD_TYPE));
    static final VarHandle TX_NUM_HANDLE = BASIC_LAYOUT.varHandle(groupElement(TX_NUM));
    static final VarHandle BLOCK_NUMBER_HANDLE = BASIC_LAYOUT.varHandle(groupElement(BLOCK_NUMBER));
    static final VarHandle FILENAME_LENGTH_HANDLE = BASIC_LAYOUT.varHandle(groupElement(FILENAME_LENGTH));
    static final VarHandle OLD_VAL_OFF_HANDLE = BASIC_LAYOUT.varHandle(groupElement(OFFSET));

    public static MemoryLayout createStringRecordMemoryLayout(final int fileNameLength, final int oldValLength, final int newValLength) {

        return MemoryLayout.structLayout(
                BASIC_LAYOUT,
                ValueLayout.JAVA_INT.withName(OLD_VAL_LENGTH),
                ValueLayout.JAVA_INT.withName(NEW_VAL_LENGTH),
                MemoryLayout.sequenceLayout(fileNameLength, ValueLayout.JAVA_CHAR).withName(FILENAME),
                MemoryLayout.sequenceLayout(oldValLength, ValueLayout.JAVA_CHAR).withName(OLD_VAL),
                MemoryLayout.sequenceLayout(newValLength, ValueLayout.JAVA_CHAR).withName(NEW_VAL)
        );
    }

    public static MemoryLayout createIntRecordMemoryLayout(final int fileNameLength) {

        return MemoryLayout.structLayout(
                BASIC_LAYOUT,
                ValueLayout.JAVA_INT.withName(NEW_VAL),
                MemoryLayout.sequenceLayout(fileNameLength, ValueLayout.JAVA_CHAR.withName(FILENAME))
        );
    }

    public static MemoryLayout createCheckPointRecordMemoryLayout(final int fileNameLength) {

        return MemoryLayout.structLayout(
                BASIC_LAYOUT,
                ValueLayout.JAVA_INT.withName(NEW_VAL),
                MemoryLayout.sequenceLayout(fileNameLength, ValueLayout.JAVA_CHAR.withName(FILENAME))
        );
    }

    public static void setBasicLayout(final MemorySegment memorySegment,
                                      final int recType,
                                      final long txNum,
                                      final int blockNum,
                                      final int fileNameLen,
                                      final int oldValOff) throws Throwable {

            RECORD_TYPE_HANDLE.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, recType);
            BLOCK_NUMBER_HANDLE.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, blockNum);
            FILENAME_LENGTH_HANDLE.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, fileNameLen);
            OLD_VAL_OFF_HANDLE.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, oldValOff);
            TX_NUM_HANDLE.toMethodHandle(VarHandle.AccessMode.SET).invokeWithArguments(memorySegment, 0L, txNum);


    }
}
