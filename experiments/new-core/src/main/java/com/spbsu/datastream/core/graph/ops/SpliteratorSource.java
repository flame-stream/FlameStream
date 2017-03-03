package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.Source;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

import java.util.Objects;
import java.util.Spliterator;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class SpliteratorSource<T> extends Source {
  private final Spliterator<DataItem<T>> spliterator;

  public SpliteratorSource(final Spliterator<DataItem<T>> spliterator) {
    this.spliterator = spliterator;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final SpliteratorSource<?> that = (SpliteratorSource<?>) o;
    return Objects.equals(spliterator, that.spliterator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spliterator);
  }

  @Override
  public String toString() {
    return "SpliteratorSource{" + "spliterator=" + spliterator +
            '}';
  }

  @Override
  public GraphStageLogic logic() {
    return new GraphStageLogic<Object, T>() {
      @Override
      public void onStart() {
        spliterator.forEachRemaining(item -> push(outPort(), item));
      }
    };
  }

  @Override
  public Graph deepCopy() {
    return new SpliteratorSource<>(spliterator);
  }
}
