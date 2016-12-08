package com.spbsu.datastream.core.inference;

import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.job.*;
import com.spbsu.datastream.example.bl.UserGrouping;
import com.spbsu.datastream.example.bl.counter.CountUserEntries;
import com.spbsu.datastream.example.bl.counter.UserCounter;
import org.jooq.lambda.Seq;

import javax.inject.Inject;
import java.util.List;
import java.util.function.Function;

/**
 * Created by marnikitta on 28.11.16.
 */
public class SimpleBuilder implements JobaBuilder {
  @Inject
  private final TypeCollection collection = DataStreamsContext.typeCollection;

  @Override
  public Joba build(final DataType from, final DataType to) throws TypeUnreachableException {
    if ("UsersLog".equals(from.name()) && "Frequencies".equals(to.name())) {
      final MergeJoba merge = new MergeJoba(collection.forName("Merge(UsersLog, States)"), new IdentityJoba(from));
      final GroupingJoba grouping = new GroupingJoba(merge, collection.forName("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
      final Joba states = new FilterJoba(grouping, collection.forName("UserCounter"), new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
      final ReplicatorJoba result = new ReplicatorJoba(states);
      merge.add(result);
      return result;
    } else throw new TypeUnreachableException();
  }
}

