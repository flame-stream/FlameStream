package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

import java.util.Objects;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class StatefulFilter<T, R, S extends State> extends Processor {
  private final StatefulFunction<T, R, S> statefulFunction;
  private final Hash<T> hash;

  public StatefulFilter(final StatefulFunction<T, R, S> statefulFunction, final Hash<T> hash) {
    this.statefulFunction = statefulFunction;
    this.hash = hash;
  }

  public StatefulFunction<T, R, S> statefulFunction() {
    return statefulFunction;
  }

  public Hash<T> hash() {
    return hash;
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

  @Override
  public GraphStageLogic logic() {
    return null;
  }

  @Override
  public Graph deepCopy() {
    return new StatefulFilter<>(statefulFunction, hash);
  }
}
