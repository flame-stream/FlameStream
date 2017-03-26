package com.spbsu.datastream.core.graph.ops;

import java.util.function.BiFunction;

public interface StatefulFunction<T, R, S> extends BiFunction<T, S, StatefulFunction.StatefulResult<R, S>> {


  class StatefulResult<R, S> {
    private final S state;
    private final R result;

    public StatefulResult(final S state, final R result) {
      this.state = state;
      this.result = result;
    }

    public S state() {
      return state;
    }

    public R result() {
      return result;
    }

    @Override
    public String toString() {
      return "StatefulResult{" + "state=" + state +
              ", result=" + result +
              '}';
    }
  }
}
