package org.berkerdb.db.buffer;

import org.berkerdb.db.file.Block;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public abstract class AbstractBufferManager {

    protected final Buffer[] bufferPool;

    private final long STANDART_WAIT_TIME = 10;
    private long numOfAvailableBuffer;

    public AbstractBufferManager(final int bufferCount) {
        this.bufferPool = new Buffer[bufferCount];

        for (int i = 0; i < bufferPool.length; i++) {
            bufferPool[i] = new Buffer();
        }

        numOfAvailableBuffer = bufferCount;
    }

    public synchronized Buffer pin(final Block block) {
        Buffer buffer = getBlock(block);

        final long startTime = System.currentTimeMillis();

        while (buffer == null && waitTimeReached(startTime)) {
            try {
                wait(TimeUnit.SECONDS.toMillis(STANDART_WAIT_TIME));
                buffer = getBlock(block);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return Optional.ofNullable(buffer).orElseThrow(BufferPoolFullException::new);
    }

    public synchronized Buffer pinNew(final String fileName) {
        Buffer buffer = getUnpinnedBuffer();

        final long startTime = System.currentTimeMillis();

        while (buffer == null && waitTimeReached(startTime)) {
            try {
                wait(TimeUnit.SECONDS.toMillis(STANDART_WAIT_TIME));
                buffer = getUnpinnedBuffer();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (buffer != null) {
            buffer.assignNew(fileName);
            numOfAvailableBuffer--;
            return buffer;
        }
        throw new BufferPoolFullException();
    }

    public synchronized void unpin(final Block block) {
        for (Buffer buffer : bufferPool) {
            if (buffer.isPinned() && buffer.getCurrentBlock().equals(block)) {
                buffer.unpin();
                if (!buffer.isPinned()) {
                    buffer.lastUnpinned.getAndSet(Instant.now().toEpochMilli());
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
            buffer.pin();
            return buffer;
        } else {
            Buffer buff = getUnpinnedBuffer();
            if (buff != null) {
                buff.assignToBlock(block);
                numOfAvailableBuffer--;
                buff.pin();
                return buff;
            }
        }
        return null;
    }
    protected abstract Buffer getUnpinnedBuffer();
    private Buffer getUnpinnedBufferNaive() {
        for (Buffer buff : bufferPool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }

    //May be replaced with plain for loop due to performance concerns.
    private Buffer getLeastRecentlyReplacedUnpinnedBuffer() {
        return Arrays.stream(bufferPool)
                .filter(Predicate.not(Buffer::isPinned))
                .min(Comparator.comparing(buffer -> buffer.lastAssigned.get()))
                .orElseThrow(RuntimeException::new);
    }

    //May be replaced with plain for loop due to performance concerns.
    private Buffer getLeastRecentlyUnpinnedBuffer() {
        return Arrays.stream(bufferPool)
                .filter(Predicate.not(Buffer::isPinned))
                .min(Comparator.comparing(buffer -> buffer.lastUnpinned.get()))
                .orElseThrow(RuntimeException::new);
    }


    public synchronized long getNumOfAvailableBuffer() {
        return numOfAvailableBuffer;
    }

    private boolean waitTimeReached(final long startTimeMillis) {
        return System.currentTimeMillis() - startTimeMillis <= TimeUnit.SECONDS.toMillis(STANDART_WAIT_TIME);
    }


    private Buffer findExistingBlock(final Block block) {
        for (Buffer buffer : bufferPool) {
            if (buffer.getCurrentBlock() != null && buffer.getCurrentBlock().equals(block)) {
                return buffer;
            }
        }
        return null;
    }
}
