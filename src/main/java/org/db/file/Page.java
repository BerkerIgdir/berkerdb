package org.db.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    static final int BLOCK_SIZE = 400;
    private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    private static int STRING_SIZE(int n) {
        return (int) (INT_SIZE + n * Charset.defaultCharset().newEncoder().maxBytesPerChar());
    }

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(BLOCK_SIZE);

    private final FileManager fileManager;

    public Page(final FileManager manager) {
        fileManager = manager;
    }

    public synchronized void append(final String fileName) {
        fileManager.append(buffer, fileName);
    }

    public synchronized void setStr(final Block block, int off, String val) {
        final int strSize = STRING_SIZE(val.length());

        buffer.putInt(off, strSize);
        buffer.put(off + INT_SIZE, val.getBytes(StandardCharsets.UTF_8));

        fileManager.write(buffer, block);
    }

    public synchronized String getStr(Block block, int off) {
        fileManager.read(buffer, block);

        final int strSize = buffer.getInt(off);
        final byte[] bytes = new byte[strSize - INT_SIZE];
        buffer.get(off + INT_SIZE, bytes);

        return new String(bytes);
    }

    public synchronized void setInt(final Block block, int off, int val) {
        buffer.putInt(off, val);
        fileManager.write(buffer, block);
    }

    public synchronized int getInt(Block block, int off) {
        fileManager.read(buffer, block);
        return buffer.getInt(off);
    }

    public synchronized void setBool(final Block block, int off, boolean val) {
        setInt(block, off, val ? 1 : 0);
    }

    public synchronized boolean getBool(Block block, int off) {
        return getInt(block, off) == 1;
    }
}
