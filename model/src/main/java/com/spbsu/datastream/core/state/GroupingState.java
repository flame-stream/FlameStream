package com.spbsu.datastream.core.state;

import com.spbsu.datastream.core.DataItem;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 14.11.16.
 */
public class GroupingState extends TLongObjectHashMap<List<GroupingState.Bucket>> {

  private Optional<Bucket> searchBucket(long hash, DataItem item) {
    return Stream.of(get(hash))
            .flatMap(state -> state != null ? state.stream() : Stream.empty())
            .filter(bucket -> bucket.isEmpty() || grouping.equals(bucket.get(0), item)).findAny();
  }

  public static class Bucket extends ArrayList<DataItem> {
    public Bucket(final int initialCapacity) {
      super(initialCapacity);
    }

    public Bucket() {
    }

    public Bucket(final Collection<? extends DataItem> c) {
      super(c);
    }
  }
}
