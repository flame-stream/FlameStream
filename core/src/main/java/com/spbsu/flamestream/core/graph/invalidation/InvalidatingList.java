package com.spbsu.flamestream.core.graph.invalidation;

import com.spbsu.flamestream.core.data.DataItem;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * User: Artem
 * Date: 01.11.2017
 */
public abstract class InvalidatingList<E> implements List<DataItem<E>> {

  /**
   * Inserts data item in list according to its meta
   *
   * @return position of inserted item within list
   */
  public abstract int insert(DataItem<E> dataItem);

  @Override
  public boolean addAll(int index, @NotNull Collection<? extends DataItem<E>> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataItem<E> set(int index, DataItem<E> element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int index, DataItem<E> element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataItem<E> remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceAll(UnaryOperator<DataItem<E>> operator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sort(Comparator<? super DataItem<E>> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeIf(Predicate<? super DataItem<E>> filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(DataItem<E> tDataItem) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends DataItem<E>> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }
}
