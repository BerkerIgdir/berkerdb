package org.berkerdb.db.buffer;

import java.util.Arrays;
import java.util.Comparator;

import java.util.function.Predicate;

public class NaiveBufferManager extends AbstractBufferManager {

    public NaiveBufferManager(final int bufferCount) {
        super(bufferCount);
    }

    @Override
    protected Buffer getUnpinnedBuffer() {
        for (Buffer buff : bufferPool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }



    //May be replaced with plain for loop due to performance concerns.
    private Buffer getLeastRecentlyUnpinnedBuffer() {
        return Arrays.stream(bufferPool)
                .filter(Predicate.not(Buffer::isPinned))
                .min(Comparator.comparing(buffer -> buffer.lastUnpinned.get()))
                .orElseThrow(RuntimeException::new);
    }

}
