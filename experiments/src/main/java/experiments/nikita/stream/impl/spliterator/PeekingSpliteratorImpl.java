package experiments.nikita.stream.impl.spliterator;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 04.11.16.
 */
public class PeekingSpliteratorImpl<T> implements PeekingSpliterator<T> {
  private T peek;

  private final Spliterator<T> base;

  public PeekingSpliteratorImpl(final Spliterator<T> base) {
    this.base = base;
  }

  @Override
  public boolean tryPeekAdvance(final Consumer<? super T> action) {
    if (peek != null) {
      action.accept(peek);
      return true;
    } else if (this.base.tryAdvance(t -> peek = t)) {
      return tryPeekAdvance(action);
    } else {
      return false;
    }
  }

  @Override
  public boolean tryAdvance(final Consumer<? super T> action) {
    if (peek != null) {
      action.accept(peek);
      peek = null;
      return true;
    } else {
      return base.tryAdvance(action);
    }
  }

  @Override
  public Spliterator<T> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return base.estimateSize();
  }

  @Override
  public int characteristics() {
    return base.characteristics();
  }
}
