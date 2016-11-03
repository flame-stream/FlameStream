package experiments.interfaces.artem.iterators.impl;

import experiments.interfaces.artem.iterators.CommitFailIterator;
import experiments.interfaces.artem.mockstream.DataItem;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ReplicatingIterator<T extends DataItem> implements CommitFailIterator<T> {
    private Iterator<T> iterator;
    private BlockingQueue<T> buffer;
    private BlockingQueue<T> stableBuffer;
    private boolean readFromBuffer;

    public ReplicatingIterator(Iterator<T> iterator) {
        if (iterator == null) {
            throw new IllegalArgumentException("iterator is null");
        }

        this.iterator = iterator;
        buffer = new LinkedBlockingQueue<>();
        stableBuffer = new LinkedBlockingQueue<>();
        readFromBuffer = false;
    }

    @Override
    public boolean hasNext() {
        return ((readFromBuffer && !buffer.isEmpty()) || iterator.hasNext());
    }

    @Override
    public T next() {
        if (readFromBuffer && !buffer.isEmpty()) {
            return buffer.poll(); //maybe it is reasonable to handle memory limitations and use take...
        } else {
            if (readFromBuffer) {
                buffer = new LinkedBlockingQueue<>(stableBuffer); //shallow copy, not optimal solution?
                stableBuffer.clear(); //redundant?
                readFromBuffer = false;
            }

            T next = iterator.next();
            buffer.add(next); //maybe it is reasonable to handle memory limitations and use offer...
            return next;
        }
    }

    @Override
    public void commit() {
        if (readFromBuffer) {
            stableBuffer = new LinkedBlockingQueue<>(buffer);
        } else {
            buffer.clear();
        }
    }

    @Override
    public void fail() {
        if (readFromBuffer) {
            buffer = new LinkedBlockingQueue<>(stableBuffer);
        } else {
            stableBuffer = new LinkedBlockingQueue<>(buffer);
        }
        readFromBuffer = true;
    }
}
