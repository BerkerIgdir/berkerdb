package org.berkerdb.db.file;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.StampedLock;

import static org.berkerdb.Main.DB;

public class Page {
    public static final int BLOCK_SIZE = 400;

    public static int STRING_SIZE(int n) {
        return (int) (Integer.BYTES + n * Charset.defaultCharset().newEncoder().maxBytesPerChar());
    }

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private final MemorySegment MEMORY_BLOCK = Arena.global().allocate(BLOCK_SIZE);

    private final StampedLock stampedLock = new StampedLock();
    private final FileManager fileManager = DB().getFileManager();

    public synchronized void read(final Block block) {
        fileManager.read(buffer, block);
    }

    public synchronized void write(final Block block) {
        fileManager.write(buffer, block);
    }

    public synchronized void setByteArray(final int off, final byte[] bytes) {
        final int byteSize = bytes.length;
        buffer.position(off);
        buffer.putInt(byteSize);
        buffer.put(bytes);
    }

    public synchronized byte[] getByteArray(final int off) {
        buffer.position(off);
        final int size = buffer.getInt();
        final byte[] bytes = new byte[size];
        buffer.get(bytes);
        return bytes;
    }

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

//    public void getIntArray(MemorySegment srcSeg, long srcOffsetBytes, int[] dstArr, long dstIndex, long numInts) {
//        MemorySegment srcSegSlice = srcSeg.asSlice(srcOffsetBytes, numInts << 2);
//        MemorySegment dstSeg = MemorySegment.ofArray(dstArr);
//        MemorySegment dstSegSlice = dstSeg.asSlice(dstIndex << 2, numInts << 2);
//        dstSegSlice.copyFrom(srcSegSlice);
//    }


    public synchronized void append(final String fileName) {
        fileManager.append(buffer, fileName);
    }

    public synchronized void setStr(int off, String val) {
        setByteArray(off, val.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized String getStr(int off) {
        return new String(getByteArray(off));
    }

    public synchronized void setInt(int off, int val) {
        buffer.putInt(off, val);
    }

    public synchronized int getInt(int off) {
        return buffer.getInt(off);
    }

    public synchronized void setBool(final int off, final boolean val) {
        buffer.position(off);
        buffer.put(val ? (byte) 1 : (byte) 0);
    }

    public synchronized boolean getBool(final int off) {
        return getInt(off) == 1;
    }

    public synchronized int lastBlockNum(String fileName) {
        return fileManager.lastBlockNum(fileName);
    }

    public synchronized long getFileSize(final String fileName) {
        return fileManager.getFileSize(fileName);
    }

    public int bufferPos() {
        return buffer.position();
    }

    public void close(){
        MEMORY_BLOCK.unload();

    }
}
