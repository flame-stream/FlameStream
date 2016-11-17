package com.spbsu.datastream.core.inference;

import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.TypeNotSupportedException;
import com.spbsu.datastream.core.job.*;
import com.spbsu.datastream.example.bl.UserContainer;
import com.spbsu.datastream.example.bl.UserGrouping;
import com.spbsu.datastream.example.bl.sql.SelectUserEntries;
import com.spbsu.datastream.example.bl.sql.UserSelector;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Created by Artem on 15.11.2016.
 */
public class SqlInference {
  @Nullable
  public DataType type(String name) {
    return new DataType.Stub(name);
  }

  @SuppressWarnings("UnusedParameters")
  public <T>Joba select(DataType sourceDataType, Function<T, T> whereFilter) throws TypeNotSupportedException {
    if ("UsersLog".equals(sourceDataType.name())) {
      final MergeJoba merge = new MergeJoba(type("Merge(UsersLog, States(Select))"), new IdentityJoba(sourceDataType));
      final FilterJoba selectJoba = new FilterJoba(merge, type("Select"), whereFilter, UserContainer.class, UserContainer.class);
      final GroupingJoba grouping = new GroupingJoba(selectJoba, type("Group(Merge(UsersLog, States(Select)), UserHash, 2)"), new UserGrouping(), 2);
      final FilterJoba states = new FilterJoba(grouping, type("UserSelector"), new SelectUserEntries(), RuntimeUtils.findTypeParameters(SelectUserEntries.class, Function.class)[0], UserSelector.class);
      final ReplicatorJoba result = new ReplicatorJoba(states);
      merge.add(result);
      return result;
    } else {
      throw new TypeNotSupportedException();
    }
  }
}
