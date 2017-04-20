package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.HashFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class GroupingResult<T> {
  public static final HashFunction<GroupingResult> HASH_FUNCTION = new HashFunction<GroupingResult>() {
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
  private final List<T> payload;

  public GroupingResult(final List<T> payload, final int hash) {
    this.payload = new ArrayList<>(payload);
    this.hash = hash;
  }

  public List<T> payload() {
    return Collections.unmodifiableList(payload);
  }

  public int rootHash() {
    return hash;
  }
}
