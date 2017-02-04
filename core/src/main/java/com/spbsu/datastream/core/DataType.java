package com.spbsu.datastream.core;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Collections;
import java.util.Set;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public interface DataType {
  DataType UNTYPED = new DataType() {
    @Override
    public String name() {
      return "untyped";
    }

    @Override
    public Set<Class<? extends Condition>> conditions() {
      return Collections.emptySet();
    }
  };

  String name();

  Set<Class<? extends Condition>> conditions();

  class Stub implements DataType {
    private String name;

    @Override
    public String name() {
      return name;
    }

    @Override
    public Set<Class<? extends Condition>> conditions() {
      return Collections.emptySet();
    }

    public Stub(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;

      if (o == null || getClass() != o.getClass()) return false;

      Stub stub = (Stub) o;

      return new EqualsBuilder()
              .append(name, stub.name)
              .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
              .append(name)
              .toHashCode();
    }
  }
}
