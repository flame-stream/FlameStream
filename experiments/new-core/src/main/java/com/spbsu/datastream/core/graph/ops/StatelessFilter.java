package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

import java.util.Objects;
import java.util.function.Function;

/**
 * Created by marnikitta on 2/7/17.
 */
public class StatelessFilter<T, R> extends Processor {
  private final Function<T, R> function;

  public StatelessFilter(final Function<T, R> function) {
    this.function = function;
  }

  public Function<T, R> function() {
    return function;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    final StatelessFilter<?, ?> that = (StatelessFilter<?, ?>) o;
    return Objects.equals(function, that.function);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), function);
  }

  @Override
  public String toString() {
    return "StatelessFilter{" + "function=" + function +
            ", " + super.toString() + '}';
  }

  @Override
  public GraphStageLogic logic() {
    return null;
  }

  @Override
  public Graph deepCopy() {
    return new StatelessFilter<>(function);
  }
}

