package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.HashFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public final class GroupingResult<T> {
  public static final HashFunction<GroupingResult> HASH_FUNCTION = new HashFunction<GroupingResult>() {
    @Override
    public boolean equal(GroupingResult o1, GroupingResult o2) {
      return o1.rootHash() == o2.rootHash();
    }

    @Override
    public int hash(GroupingResult value) {
      return value.rootHash();
    }
  };

  private final int hash;
  private final List<T> payload;

  public GroupingResult(List<T> payload, int hash) {
    this.payload = new ArrayList<>(payload);
    this.hash = hash;
  }

  public List<T> payload() {
    return Collections.unmodifiableList(this.payload);
  }

  public int rootHash() {
    return this.hash;
  }

  @Override
  public String toString() {
    return "GR{" + this.payload + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final GroupingResult<?> that = (GroupingResult<?>) o;
    return this.hash == that.hash &&
            Objects.equals(this.payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.hash, this.payload);
  }
}
