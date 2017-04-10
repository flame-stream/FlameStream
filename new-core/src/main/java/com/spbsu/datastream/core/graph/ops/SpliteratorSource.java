package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.Source;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Spliterator;
import java.util.concurrent.TimeUnit;

public final class SpliteratorSource<T> extends Source<T> {
  private final Spliterator<T> spliterator;
  private final HashFunction<? super T> initHash;

  public SpliteratorSource(final Spliterator<T> spliterator,
                           final HashFunction<? super T> initHash) {
    this.spliterator = spliterator;
    this.initHash = initHash;
  }

  @Override
  public void onStart(final AtomicHandle handler) {
    spliterator.forEachRemaining(item -> {
      final Meta now = new Meta(
              System.currentTimeMillis(),
              handler.incrementLocalTimeAndGet(),
              initHash.hash(item));

      handler.push(outPort(), new PayloadDataItem<>(now, item));
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
