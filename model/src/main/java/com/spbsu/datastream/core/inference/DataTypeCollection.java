package com.spbsu.datastream.core.inference;

import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.impl.TypeConvertersCollection;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.*;
import com.spbsu.datastream.core.TypeUnreachableException;
import com.spbsu.datastream.example.bl.CountUserEntries;
import com.spbsu.datastream.example.bl.UserCounter;
import com.spbsu.datastream.example.bl.UserGrouping;
import com.spbsu.datastream.example.bl.UserQuery;
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
      final NewGroupingJoba grouping = new NewGroupingJoba(merge, type("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
      final Joba states = new FilterJoba(grouping, type("UserCounter"), new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
      final ReplicatorJoba result = new ReplicatorJoba(states);
      merge.add(result);
      return result;
    }
    else throw new TypeUnreachableException();
  }
}
