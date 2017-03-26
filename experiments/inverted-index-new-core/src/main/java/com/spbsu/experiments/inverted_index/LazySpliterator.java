package com.spbsu.experiments.inverted_index;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by marnikitta on 04.11.16.
 */
public class LazySpliterator<T, T_SPLITR extends Spliterator<T>> implements Spliterator<T> {
  private final Supplier<? extends T_SPLITR> supplier;

  private T_SPLITR s;

  public LazySpliterator(Supplier<? extends T_SPLITR> supplier) {
    this.supplier = supplier;
  }

  private T_SPLITR get() {
    if (s == null) {
      s = supplier.get();
    }
    return s;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T_SPLITR trySplit() {
    return (T_SPLITR) get().trySplit();
  }

  @Override
  public boolean tryAdvance(Consumer<? super T> consumer) {
    return get().tryAdvance(consumer);
  }

  @Override
  public void forEachRemaining(Consumer<? super T> consumer) {
    get().forEachRemaining(consumer);
  }

  @Override
  public long estimateSize() {
    return get().estimateSize();
  }

  @Override
  public int characteristics() {
    return get().characteristics();
  }

  @Override
  public Comparator<? super T> getComparator() {
    return get().getComparator();
  }

  @Override
  public long getExactSizeIfKnown() {
    return get().getExactSizeIfKnown();
  }

  @Override
  public String toString() {
    return getClass().getName() + "[" + get() + "]";
  }
}
