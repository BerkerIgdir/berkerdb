package org.berkerdb.db.file;


import java.io.Closeable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static org.berkerdb.Main.DB;
import static org.berkerdb.db.file.Page.BLOCK_SIZE;

// Page implementation for only single thread usage.
// No lock is used. Behavioural consistency is not expected in multi-threaded cases,
public class LocalMemoryPage implements Closeable {

    private final Arena memoryArena = Arena.ofConfined();
    private final MemorySegment MEMORY_BLOCK = memoryArena.allocate(BLOCK_SIZE);

    private final FileManager fileManager = DB().getFileManager();

    public void setByteArrayToMemory(final int off, final byte[] bytes) {
        final MemorySegment sourceSegment = MemorySegment.ofArray(bytes);
        MEMORY_BLOCK.set(ValueLayout.JAVA_INT, off, bytes.length);
        MemorySegment.copy(sourceSegment, 0, MEMORY_BLOCK, off + Integer.BYTES, bytes.length);
    }

    public void setIntToMemory(final int off, final int val) {
        MEMORY_BLOCK.set(ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN), off, val);
    }

    public byte[] getByteArrayFromMemory(final int off) {
        var byteArraySize = MEMORY_BLOCK.get(ValueLayout.JAVA_INT, off);
        byte[] bytes = new byte[byteArraySize];
        var byteBackedMem = MemorySegment.ofArray(bytes);
        var sourceSlice = MEMORY_BLOCK.asSlice(off + Integer.BYTES, byteArraySize);
        byteBackedMem.copyFrom(sourceSlice);

        return bytes;
    }

    public int getIntFromMemory(final int off) {
        return MEMORY_BLOCK.get(ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN), off);
    }

    public String getStringFromMemory(final int off) {
        var byteArraySize = MEMORY_BLOCK.get(ValueLayout.JAVA_INT, off);
        byte[] bytes = new byte[byteArraySize];
        var byteBackedMem = MemorySegment.ofArray(bytes);
        var sourceSlice = MEMORY_BLOCK.asSlice(off + Integer.BYTES, byteArraySize);
        byteBackedMem.copyFrom(sourceSlice);

        return new String(byteBackedMem.toArray(ValueLayout.JAVA_BYTE));
    }

    public void read(final Block block) {
        fileManager.read(MEMORY_BLOCK.asByteBuffer(), block);
    }

//    public synchronized void write(final Block block) {
//        fileManager.write(MEMORY_BLOCK.asByteBuffer(), block);
//    }

    public void close() {
        //MEMORY_BLOCK.unload();
        memoryArena.close();
    }
}
