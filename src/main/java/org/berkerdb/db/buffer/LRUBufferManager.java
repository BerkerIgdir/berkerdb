package org.berkerdb.db.buffer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;

public class LRUBufferManager extends AbstractBufferManager{

    public LRUBufferManager(int bufferCount) {
        super(bufferCount);
    }

    //May be replaced with plain for loop due to performance concerns.
    @Override
    protected Buffer getUnpinnedBuffer() {
        return Arrays.stream(bufferPool)
                .filter(Predicate.not(Buffer::isPinned))
                .min(Comparator.comparing(buffer -> buffer.lastUnpinned.get()))
                .orElseThrow(RuntimeException::new);
    }
}
