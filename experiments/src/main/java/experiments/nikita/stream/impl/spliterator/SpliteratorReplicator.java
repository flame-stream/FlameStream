package experiments.nikita.stream.impl.spliterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 04.11.16.
 */
public class SpliteratorReplicator<S> {
  private final Spliterator<S> root;
  private final List<Queue<S>> consumers = new ArrayList<>();

  public SpliteratorReplicator(final Spliterator<S> root) {
    this.root = root;
  }

  private boolean pushNext() {
    return root.tryAdvance(l -> consumers.forEach(q -> q.add(l)));
  }

  public Spliterator<S> split() {
    final Queue<S> localQueue = new LinkedBlockingQueue<>();
    consumers.add(localQueue);

    return new Spliterator<S>() {

      private boolean tryPoll(final Consumer<? super S> action) {
        if (!localQueue.isEmpty()) {
          action.accept(localQueue.remove());
          return true;
        } else {
          return false;
        }
      }

      @Override
      public boolean tryAdvance(final Consumer<? super S> action) {
        if (tryPoll(action)) {
          return true;
        } else {
          pushNext();
          return tryPoll(action);
        }
      }

      @Override
      public Spliterator<S> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return -1;
      }

      @Override
      public int characteristics() {
        return root.characteristics();
      }
    };
  }
}
