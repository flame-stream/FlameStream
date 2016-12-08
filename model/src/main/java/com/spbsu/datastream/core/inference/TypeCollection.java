package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.type.TypeTemplate;

import java.beans.Transient;
import java.util.Collection;
import java.util.NoSuchElementException;

/**
 * Created by marnikitta on 28.11.16.
 */
public interface TypeCollection {
  DataType type(String name) throws NoSuchElementException;

  TypeTemplate template(String name) throws NoSuchElementException;


  Transformation transformation(String name) throws NoSuchElementException;

  void addType(DataType type);

  Collection<DataType> loadTypes();
}
