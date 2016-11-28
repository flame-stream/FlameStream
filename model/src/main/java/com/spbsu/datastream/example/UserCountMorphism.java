package com.spbsu.datastream.example;

import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.inference.DataTypeCollection;
import com.spbsu.datastream.core.inference.Morphism;
import com.spbsu.datastream.core.job.*;
import com.spbsu.datastream.example.bl.UserGrouping;
import com.spbsu.datastream.example.bl.counter.CountUserEntries;
import com.spbsu.datastream.example.bl.counter.UserCounter;

import java.util.function.Function;

/**
 * Created by marnikitta on 28.11.16.
 */
public class UserCountMorphism implements Morphism {
  private final DataTypeCollection collection = DataStreamsContext.typeCollection;

  @Override
  public DataType consumes() {
    return collection.forName("UsersLog");
  }

  @Override
  public DataType supplies() {
    return collection.forName("Frequencies");
  }

  @Override
  public Joba apply(final Joba joba) {
    final MergeJoba merge = new MergeJoba(collection.forName("Merge(UsersLog, States)"), joba);
    final GroupingJoba grouping = new GroupingJoba(merge, collection.forName("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
    final Joba states = new FilterJoba(grouping, collection.forName("UserCounter"), new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
    final ReplicatorJoba result = new ReplicatorJoba(states);
    merge.add(result);
    return result;
  }
}
