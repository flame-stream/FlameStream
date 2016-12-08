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

  public DataType forName(String name) throws NoSuchElementException {
    return new DataType.Stub(name);
  }

  @Override
  public void addType(final DataType type) {
    types.put(type.name(), type);
  }

  @Override
  public Collection<DataType> loadTypes() {
    return Collections.unmodifiableCollection(types.values());
  }

}
