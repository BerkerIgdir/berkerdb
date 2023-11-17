package org.berkerdb.db.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.berkerdb.Main;

import static org.berkerdb.Main.DB;

public class Page {
    public static final int BLOCK_SIZE = 400;
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    public static int STRING_SIZE(int n) {
        return (int) (INT_SIZE + n * Charset.defaultCharset().newEncoder().maxBytesPerChar());
    }

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(BLOCK_SIZE);

    private final FileManager fileManager = DB().getFileManager();

    public synchronized void read(final Block block) {
        fileManager.read(buffer, block);
    }

    public synchronized void write(final Block block) {
        fileManager.write(buffer, block);
    }

    public synchronized void setByteArray(final int off, final byte[] bytes) {
        buffer.position(off);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    public synchronized byte[] getByteArray(final int off) {
        buffer.position(off);
        final int size = buffer.getInt();
        final byte[] bytes = new byte[size];
        buffer.get(bytes);
        return bytes;
    }

    public synchronized void append(final String fileName) {
        fileManager.append(buffer, fileName);
    }

    public synchronized void setStr(int off, String val) {
        final int strSize = STRING_SIZE(val.length());

        buffer.putInt(off, strSize);
        buffer.put(off + INT_SIZE, val.getBytes(StandardCharsets.UTF_8));

    }

    public synchronized String getStr(int off) {
        final int strSize = buffer.getInt(off);
        final byte[] bytes = new byte[strSize - INT_SIZE];
        buffer.get(off + INT_SIZE, bytes);

        return new String(bytes);
    }

    public synchronized void setInt(int off, int val) {
        buffer.putInt(off, val);
    }

    public synchronized int getInt(int off) {
        return buffer.getInt(off);
    }

    public synchronized void setBool(final int off, final boolean val) {
        setInt(off, val ? (byte) 1 : (byte) 0);
    }

    public synchronized boolean getBool(final int off) {
        buffer.position(off);
        return buffer.get() == 1;
    }

    public synchronized int lastBlockNum(String fileName) {
        return (int) (fileManager.getFileSize(fileName) / BLOCK_SIZE);
    }
}
