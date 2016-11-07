package experiments.interfaces.nikita.stream.impl.spliterator;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 02.11.16.
 */
public class QueueSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<T> queue;

    private final long timeoutMills;

    public QueueSpliterator(final BlockingQueue<T> queue, final long timeoutMills) {
        this.queue = queue;
        this.timeoutMills = timeoutMills;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        try {
            final T next = this.queue.poll(this.timeoutMills, TimeUnit.MILLISECONDS);
            if (next == null) {
                return false;
            }
            action.accept(next);
            return true;
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.ORDERED;
    }
}
