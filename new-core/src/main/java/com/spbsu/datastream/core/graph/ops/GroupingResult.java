package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.Hashable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class GroupingResult<T extends Hashable<T>> implements Hashable<GroupingResult<T>> {
  private final List<T> list;

  private final int hash;

  public GroupingResult(final List<T> list) {
    this.list = Collections.unmodifiableList(new ArrayList<>(list));
    this.hash = list.stream().findAny().map(Hashable::hash).orElse(0);
  }

  @Override
  public int hash() {
    return hash;
  }

  @Override
  public boolean hashEquals(final GroupingResult<T> that) {
    final Optional<T> thatAny = that.list.stream().findAny();
    final Optional<T> thisAny = this.list.stream().findAny();

    if (!thisAny.isPresent() && !thatAny.isPresent()) {
      return true;
    } else if (thisAny.isPresent() && thatAny.isPresent()) {
      return thisAny.get().hashEquals(thatAny.get());
    } else {
      return false;
    }
  }
}
