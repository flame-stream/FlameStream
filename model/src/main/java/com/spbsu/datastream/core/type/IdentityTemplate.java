package com.spbsu.datastream.core.type;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.condition.Condition;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Created by marnikitta on 12/8/16.
 */
public class IdentityTemplate implements TypeTemplate {
  private final DataType type;

  public IdentityTemplate(DataType type) {
    this.type = type;
  }

  @Override
  public DataType apply(DataType... args) {
    return type;
  }

  @Override
  public int arity() {
    return 1;
  }

  @Override
  public Set<Condition> conditionsFor(int n) {
    return Collections.emptySet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IdentityTemplate that = (IdentityTemplate) o;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public String toString() {
    String sb = "IdentityTemplate{" + "type=" + type +
            '}';
    return sb;
  }
}
