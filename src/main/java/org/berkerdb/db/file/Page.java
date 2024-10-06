package org.berkerdb.db.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.berkerdb.Main.DB;

public class Page {
    public static final int BLOCK_SIZE = 200000;

    public static int STRING_SIZE(int n) {
        return (int) (Integer.BYTES + n * Charset.defaultCharset().newEncoder().maxBytesPerChar());
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

    public void clearPage() {
        buffer.clear();
    }
}
