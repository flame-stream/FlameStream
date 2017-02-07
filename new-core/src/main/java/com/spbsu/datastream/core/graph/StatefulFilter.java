package com.spbsu.datastream.core.graph;

import java.util.Objects;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class StatefulFilter<T, R, S> extends Processor {
  private final StatefulFunction<T, R, S> statefulFunction;

  public StatefulFilter(final StatefulFunction<T, R, S> statefulFunction) {
    this.statefulFunction = statefulFunction;
  }

  public StatefulFunction<T, R, S> statefulFunction() {
    return statefulFunction;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    final StatefulFilter<?, ?, ?> that = (StatefulFilter<?, ?, ?>) o;
    return Objects.equals(statefulFunction, that.statefulFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), statefulFunction);
  }

  @Override
  public String toString() {
    return "StatefulFilter{" + "statefulFunction=" + statefulFunction +
            ", " + super.toString() + '}';
  }
}
