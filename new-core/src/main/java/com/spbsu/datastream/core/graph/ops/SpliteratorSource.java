package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.Source;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Spliterator;
import java.util.concurrent.TimeUnit;

public final class SpliteratorSource<T> extends Source<T> {
  private final Spliterator<T> spliterator;

  public SpliteratorSource(final Spliterator<T> spliterator) {
    this.spliterator = spliterator;
  }

  @Override
  public void onStart(final AtomicHandle handler) {
    spliterator.forEachRemaining(item -> {
      handler.push(outPort(), new PayloadDataItem<>(Meta.now(), item));
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
