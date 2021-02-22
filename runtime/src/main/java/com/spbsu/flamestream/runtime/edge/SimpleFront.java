package com.spbsu.flamestream.runtime.edge;

import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class SimpleFront implements Front {
  private final CompletableFuture<Consumer<Object>> consumers;

  public static class Handle {
    private final Consumer<Object> consumer;
    private final EdgeContext context;
    private Meta basicMeta;
    private int childId = 0;

    public Handle(ConcurrentMap<EdgeContext, CompletableFuture<Consumer<Object>>> consumer, EdgeContext context) {
      try {
        this.consumer = consumer.computeIfAbsent(context, __ -> new CompletableFuture<>()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      this.context = context;
    }

    public synchronized void onDataItem(long time, Object object) {
      if (basicMeta != null && basicMeta.globalTime().time() > time) {
        throw new IllegalArgumentException();
      }
      if (basicMeta == null || basicMeta.globalTime().time() < time) {
        final var globalTime = new GlobalTime(time, context.edgeId());
        consumer.accept(new Heartbeat(globalTime));
        basicMeta = new Meta(globalTime);
        childId = 0;
      }
      final var dataItem = new PayloadDataItem(new Meta(basicMeta, 0, childId++), object);
      consumer.accept(dataItem);
    }

    public synchronized void unregister() {
      consumer.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, context.edgeId())));
    }
  }

  public static class Instance implements FlameRuntime.FrontInstance<SimpleFront> {
    private final Map<EdgeContext, CompletableFuture<Consumer<Object>>> consumers;

    public Instance(Map<EdgeContext, CompletableFuture<Consumer<Object>>> consumers) {this.consumers = consumers;}

    @Override
    public Class<SimpleFront> clazz() {
      return SimpleFront.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{consumers};
    }
  }

  public static class Type implements FlameRuntime.FrontType<SimpleFront, Handle> {
    private final ConcurrentMap<EdgeContext, CompletableFuture<Consumer<Object>>> consumers
            = new ConcurrentHashMap<>();

    @Override
    public Instance instance() {
      return new Instance(consumers);
    }

    @Override
    public Handle handle(EdgeContext context) {
      return new Handle(consumers, context);
    }
  }

  public SimpleFront(
          EdgeContext edgeContext,
          ConcurrentMap<EdgeContext, CompletableFuture<Consumer<Object>>> consumers
  ) {
    this.consumers = consumers.computeIfAbsent(edgeContext, __ -> new CompletableFuture<>());
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {
    if (!consumers.complete(consumer)) {
      throw new IllegalStateException();
    }
  }

  @Override
  public void onRequestNext() {
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
  }
}
