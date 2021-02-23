package com.spbsu.flamestream.example.nexmark;

import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class LatencyRearType implements FlameRuntime.RearType<LatencyRearType.Rear, LatencyRearType.Handle> {
  final ConcurrentHashMap<EdgeContext, CompletableFuture<?>> edgeContextDone = new ConcurrentHashMap<>();

  public class Instance implements FlameRuntime.RearInstance<Rear> {
    @Override
    public Class<Rear> clazz() {
      return Rear.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{LatencyRearType.this};
    }
  }

  public static class Rear implements com.spbsu.flamestream.runtime.edge.Rear {
    private final EdgeContext edgeContext;
    private final LatencyRearType type;

    public Rear(EdgeContext edgeContext, LatencyRearType type) {
      this.edgeContext = edgeContext;
      this.type = type;
    }

    Batch last = Batch.Default.EMPTY;

    @Override
    public CompletionStage<?> accept(Batch batch) {
      last = batch;
      if (batch.time().time() == Long.MAX_VALUE) {
        type.edgeContextDone.computeIfAbsent(edgeContext, __ -> new CompletableFuture<>()).complete(null);
      }
      return CompletableFuture.completedStage(null);
    }

    @Override
    public Batch last() {
      return last;
    }
  }

  public class Handle {
    private final EdgeContext context;

    public Handle(EdgeContext context) {
      this.context = context;
    }

    void await() throws InterruptedException {
      try {
        edgeContextDone.computeIfAbsent(context, __ -> new CompletableFuture<>()).get();
      } catch (ExecutionException exception) {
        throw new RuntimeException(exception);
      }
    }
  }

  @Override
  public Instance instance() {
    return new Instance();
  }

  @Override
  public Handle handle(EdgeContext context) {
    return new Handle(context);
  }
}
