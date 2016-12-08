package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.condition.Condition;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Created by marnikitta on 12/8/16.
 */
public class VarianceType implements DataType {
  private final DataType param;

  public VarianceType(DataType param) {
    this.param = param;
  }

  @Override
  public String name() {
    return "Var(" + param.name() + ")";
  }

  @Override
  public Set<Class<? extends Condition>> conditions() {
    return Collections.emptySet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VarianceType that = (VarianceType) o;
    return Objects.equals(param, that.param);
  }

  @Override
  public int hashCode() {
    return Objects.hash(param);
  }
}
