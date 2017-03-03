package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;

import java.util.Objects;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface StatefulFunction<T, R, S extends State> {
  StatefulFilterResult<R, S> process(S oldState, DataItem<T> item);

  class StatefulFilterResult<R, S> {
    private final DataItem<R> out;
    private final S newState;

    public StatefulFilterResult(final DataItem<R> out, final S newState) {
      this.out = out;
      this.newState = newState;
    }

    public DataItem<R> out() {
      return out;
    }

    public S newState() {
      return newState;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final StatefulFilterResult<?, ?> that = (StatefulFilterResult<?, ?>) o;
      return Objects.equals(out, that.out) &&
              Objects.equals(newState, that.newState);
    }

    @Override
    public int hashCode() {
      return Objects.hash(out, newState);
    }

    @Override
    public String toString() {
      return "StatefulFilterResult{" + "out=" + out +
              ", newState=" + newState +
              '}';
    }
  }
}
