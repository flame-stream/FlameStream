package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;

import java.util.*;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class DataTypeCollection implements TypeCollection {
  private final Map<String, DataType> types = new HashMap<>();

  public DataType type(String name) throws NoSuchElementException {
    return new DataType.Stub(name);
  }

  @Override
  public Morphism template(String name) throws NoSuchElementException {
    return null;
  }

  @Override
  public PolymorphicMorphism polyMorphism(String name) throws NoSuchElementException {
    return null;
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
