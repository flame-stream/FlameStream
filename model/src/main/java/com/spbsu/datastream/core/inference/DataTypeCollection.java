package com.spbsu.datastream.core.inference;

import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.job.*;
import com.spbsu.datastream.example.bl.counter.CountUserEntries;
import com.spbsu.datastream.example.bl.counter.UserCounter;
import com.spbsu.datastream.example.bl.UserGrouping;

import java.util.*;
import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class DataTypeCollection implements TypeCollection {
  private final Map<String, DataType> types = new HashMap<>();

  private final Set<Morphism> morphisms = new HashSet<>();


  public DataType forName(String name) throws NoSuchElementException {
    return new DataType.Stub(name);
  }

  @Override
  public void addMorphism(final Morphism m) {
    morphisms.add(m);
  }

  @Override
  public void addType(final DataType type) {
    types.put(type.name(), type);
  }

  @Override
  public Collection<Morphism> loadMorphisms() {
    return Collections.unmodifiableCollection(morphisms);
  }

  @Override
  public Collection<DataType> loadTypes() {
    return Collections.unmodifiableCollection(types.values());
  }

  public Joba convert(DataType from, DataType to) throws TypeUnreachableException {
//    if ("UsersLog".equals(from.name()) && "Frequencies".equals(to.name())) {
//      final MergeJoba merge = new MergeJoba(forName("Merge(UsersLog, States)"), new IdentityJoba(from));
//      final GroupingJoba grouping = new GroupingJoba(merge, forName("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
//      final Joba states = new FilterJoba(grouping, forName("UserCounter"), new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
//      final ReplicatorJoba result = new ReplicatorJoba(states);
//      merge.add(result);
//      return result;
//    } else throw new TypeUnreachableException();
    return null;
  }
}
