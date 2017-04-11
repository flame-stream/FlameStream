package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.HashFunction;

import java.util.List;

public final class GroupingResult<T> {
  public final static HashFunction<GroupingResult> HASH_FUNCTION = new HashFunction<GroupingResult>() {
    @Override
    public boolean equal(final GroupingResult o1, final GroupingResult o2) {
      return o1.rootHash() == o2.rootHash();
    }

    @Override
    public int hash(final GroupingResult value) {
      return value.rootHash();
    }
  };

  private final int hash;
  private List<T> payload;

  public GroupingResult(final List<T> payload, int hash) {
    this.payload = payload;
    this.hash = hash;
  }

  public List<T> payload() {
    return payload;
  }

  public int rootHash() {
    return hash;
  }
}
