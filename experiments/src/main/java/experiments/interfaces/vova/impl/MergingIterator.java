package experiments.interfaces.vova.impl;

import experiments.interfaces.vova.DataItem;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MergingIterator<T extends DataItem> implements Iterator<T> {
  private final Iterator<T> firstIterator;
  private final Iterator<T> secondIterator;

  private T firstIteratorNext;
  private T secondIteratorNext;

  public MergingIterator(Iterator<T> firstIterator, Iterator<T> secondIterator) {
    if (firstIterator == null || secondIterator == null) {
      throw new IllegalArgumentException("first or second iterator is null");
    }

    this.firstIterator = firstIterator;
    this.secondIterator = secondIterator;

    updateFirstIteratorNext();
    updateSecondIteratorNext();
  }

  @Override
  public boolean hasNext() {
    return (firstIteratorNext != null) || (secondIteratorNext != null);
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    T next;
    if (firstIteratorNext != null && secondIteratorNext != null) {
      if (firstIteratorNext.meta().time() < secondIteratorNext.meta().time()) {
        next = getAndUpdateFirstIteratorNext();
      } else {
        next = getAndUpdateSecondIteratorNext();
      }
    } else if (firstIteratorNext != null) {
      next = getAndUpdateFirstIteratorNext();
    } else {
      next = getAndUpdateSecondIteratorNext();
    }
    return next;
  }

  private T getAndUpdateFirstIteratorNext() {
    T next = firstIteratorNext;
    updateFirstIteratorNext();
    return next;
  }

  private T getAndUpdateSecondIteratorNext() {
    T next = secondIteratorNext;
    updateSecondIteratorNext();
    return next;
  }

  private void updateFirstIteratorNext() {
    firstIteratorNext = firstIterator.hasNext() ? firstIterator.next() : null;
  }

  private void updateSecondIteratorNext() {
    secondIteratorNext = secondIterator.hasNext() ? secondIterator.next() : null;
  }
}
