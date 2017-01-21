package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.core.Filter;

import java.lang.reflect.Field;

/**
 * Created by Artem on 23.11.2016.
 */
public class SqlWhereEqualsToFilter<T> implements Filter<T, T> {
  private final String fieldName;
  private final Object fieldValue;

  public SqlWhereEqualsToFilter(String fieldName, Object fieldValue) {
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
  }

  @Override
  public T apply(T obj) {
    try {
      final Field field = obj.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      if (field.get(obj).equals(fieldValue)) {
        return obj;
      } else {
        return null;
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format("(%s = %s)", fieldName, fieldValue);
  }

  @Override
  public boolean processOutputByElement() {
    return false;
  }
}
