package org.berkerdb.db.buffer;

import org.berkerdb.db.file.Block;

import java.util.Optional;

import java.util.concurrent.TimeUnit;

public class BufferManager {
    private final Buffer[] buffers;

    private final int STANDART_WAIT_TIME = 10;
    private long numOfAvailableBuffer;

    public BufferManager(final int bufferCount) {
        this.buffers = new Buffer[bufferCount];

        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new Buffer();
        }

        numOfAvailableBuffer = bufferCount;
    }

    public synchronized Buffer pin(final Block block) {
        Buffer buffer = getBlock(block);

        final long startTime = System.currentTimeMillis();

        while (buffer == null && !waitTimeReached(startTime)) {
            try {
                wait(TimeUnit.SECONDS.toMillis(STANDART_WAIT_TIME));
                buffer = getBlock(block);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return Optional.ofNullable(buffer).orElseThrow(RuntimeException::new);
    }

    public synchronized void unpin(final Block block) {
        for (Buffer buffer : buffers) {
            if (buffer.isPinned() && buffer.getCurrentBlock().equals(block)) {
                buffer.unpin();
                if (!buffer.isPinned()) {
                    numOfAvailableBuffer++;
                    notifyAll();
                }
            }
        }
    }

    private Buffer getBlock(final Block block) {
        Buffer buffer = findExistingBlock(block);

        if (buffer != null) {
            numOfAvailableBuffer--;
            return buffer;
        } else {
            for (Buffer buff : buffers) {
                if (!buff.isPinned()) {
                    buff.assignToBlock(block);
                    numOfAvailableBuffer--;
                    return buff;
                }
            }
        }
        return null;
    }

    public synchronized long getNumOfAvailableBuffer() {
        return numOfAvailableBuffer;
    }

    private boolean waitTimeReached(final long startTimeMillis) {
        return System.currentTimeMillis() - startTimeMillis > TimeUnit.SECONDS.toMillis(STANDART_WAIT_TIME);
    }


    private Buffer findExistingBlock(final Block block) {
        for (Buffer buffer : buffers) {
            if (buffer.getCurrentBlock() != null && buffer.getCurrentBlock().equals(block)) {
                return buffer;
            }
        }
        return null;
    }


}
