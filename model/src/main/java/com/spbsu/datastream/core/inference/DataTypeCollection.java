package com.spbsu.datastream.core.inference;

import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.job.*;
import com.spbsu.datastream.example.bl.counter.CountUserEntries;
import com.spbsu.datastream.example.bl.counter.UserCounter;
import com.spbsu.datastream.example.bl.UserGrouping;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class DataTypeCollection {
  @Nullable
  public DataType type(String name) {
    return new DataType.Stub(name);
  }

  public Joba convert(DataType from, DataType to) throws TypeUnreachableException {
    if ("UsersLog".equals(from.name()) && "Frequencies".equals(to.name())) {
      final MergeJoba merge = new MergeJoba(type("Merge(UsersLog, States)"), new IdentityJoba(from));
      final GroupingJoba grouping = new GroupingJoba(merge, type("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
      final Joba states = new FilterJoba(grouping, type("UserCounter"), new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
      final ReplicatorJoba result = new ReplicatorJoba(states);
      merge.add(result);
      return result;
    } else throw new TypeUnreachableException();
  }
}
