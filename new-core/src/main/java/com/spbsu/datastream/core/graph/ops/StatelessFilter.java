package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

import java.util.Objects;
import java.util.function.Function;

public class StatelessFilter<T, R> extends Processor<T, R> {
  private final Function<T, R> function;

  public StatelessFilter(final Function<T, R> function) {
    this.function = function;
  }

  public Function<T, R> function() {
    return function;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    @SuppressWarnings("unchecked")
    final R res = function.apply((T) item.payload());

    handler.push(outPort(), new PayloadDataItem<>(new Meta(item.meta(), this.hashCode()), res, item.rootId()));
    handler.ack(item);
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
  public Graph deepCopy() {
    return new StatelessFilter<>(function);
  }
}

