package org.berkerdb.db.buffer;

public class BufferPoolFullException extends RuntimeException {
    private static final String DEFAULT_MESSAGE = "BUFFER POOL IS FULL!";

    public BufferPoolFullException(final String m) {
        super(m);
    }

    public BufferPoolFullException() {
        super(DEFAULT_MESSAGE);
    }

}
