package org.berkerdb.db.file;


import java.io.Closeable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.locks.StampedLock;

import static org.berkerdb.Main.DB;
import static org.berkerdb.db.file.Page.BLOCK_SIZE;

//A temporary page implementation, it will replace the current one in the future.
public class LogPage implements Closeable {

    private final Arena memoryArena = Arena.ofConfined();
    private final MemorySegment MEMORY_BLOCK = memoryArena.allocate(BLOCK_SIZE);

    private final StampedLock stampedLock = new StampedLock();

    private final FileManager fileManager = DB().getFileManager();

    public void setByteArrayToMemory(final int off, final byte[] bytes) {
        final long writeLock = stampedLock.writeLock();
        try {
            final MemorySegment sourceSegment = MemorySegment.ofArray(bytes);
            MEMORY_BLOCK.set(ValueLayout.JAVA_INT, off, bytes.length);
            MemorySegment.copy(sourceSegment, 0, MEMORY_BLOCK, off + Integer.BYTES, bytes.length);
        } finally {
            stampedLock.unlockWrite(writeLock);
        }
    }

    public byte[] getByteArrayFromMemory(final int off) {
        var optReadLock = stampedLock.tryOptimisticRead();
        var byteArraySize = MEMORY_BLOCK.get(ValueLayout.JAVA_INT, off);
        byte[] bytes = new byte[byteArraySize];
        var byteBackedMem = MemorySegment.ofArray(bytes);
        var sourceSlice = MEMORY_BLOCK.asSlice(off + Integer.BYTES, byteArraySize);
        byteBackedMem.copyFrom(sourceSlice);
        if (!stampedLock.validate(optReadLock)) {
            optReadLock = stampedLock.readLock();
            try {
                //Do the exactly same things from the scratch, basically the worst case scenario :(
                byteArraySize = MEMORY_BLOCK.get(ValueLayout.JAVA_INT, off);
                bytes = new byte[byteArraySize];
                byteBackedMem = MemorySegment.ofArray(bytes);
                sourceSlice = MEMORY_BLOCK.asSlice(off + Integer.BYTES, byteArraySize);
                byteBackedMem.copyFrom(sourceSlice);
            } finally {
                stampedLock.unlockRead(optReadLock);
            }
        }
        return bytes;
    }


    public synchronized void read(final Block block) {
        fileManager.read(MEMORY_BLOCK.asByteBuffer(), block);
    }

    public synchronized void write(final Block block) {
        fileManager.write(MEMORY_BLOCK.asByteBuffer(), block);
    }

    public void close() {
        MEMORY_BLOCK.unload();
        memoryArena.close();
    }
}
