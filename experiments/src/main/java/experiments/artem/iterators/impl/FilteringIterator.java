package experiments.artem.iterators.impl;

import experiments.artem.mockstream.DataItem;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

public class FilteringIterator<T extends DataItem, R extends DataItem> implements Iterator<R> {
  private final Function<T, R> filter;
  private final Iterator<T> iterator;

  public FilteringIterator(Iterator<T> iterator, Function<T, R> filter) {
    iterator = Objects.requireNonNull(iterator);
    if (iterator == null) {
      throw new IllegalArgumentException("iterator is null");
    }
    if (filter == null) {
      throw new IllegalArgumentException("filter is null");
    }

    this.iterator = iterator;
    this.filter = filter;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public R next() {
    T next = iterator.next();
    return filter.apply(next);
  }
}
