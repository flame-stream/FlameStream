package com.spbsu.flamestream.core.front;

import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.source.SourceHandle;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public final class StreamFront<T> implements Front<T> {
  private final Stream<T> stream;

  public StreamFront(Stream<T> stream) {
    this.stream = stream;
  }

  @Override
  public void subscribe(SourceHandle<T> sourceHandle, GlobalTime from, GlobalTime to) {
    final CompletableFuture<Void> task = CompletableFuture.runAsync(() ->
            stream.forEach(e ->
                    sourceHandle.onInput(
                            new PayloadDataItem<T>(Meta.meta(new GlobalTime(System.currentTimeMillis(), 1)), e)
                    )
            )
    );

    sourceHandle.onSubscribe(new StreamSubscription(task));
  }

  private final class StreamSubscription implements FrontSubscription {
    private final CompletableFuture<Void> task;

    private StreamSubscription(CompletableFuture<Void> task) {this.task = task;}

    @Override
    public void request(long count) {
      //DO nothing
    }

    @Override
    public void cancel() {
      task.cancel(true);
    }

    @Override
    public void switchToPull() {
      //DO nothing
    }

    @Override
    public void switchToPush() {
      //DO nothing
    }
  }
}
