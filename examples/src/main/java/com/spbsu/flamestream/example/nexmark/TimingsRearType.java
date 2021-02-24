package com.spbsu.flamestream.example.nexmark;

import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

import java.text.NumberFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class TimingsRearType implements FlameRuntime.RearType<TimingsRearType.Rear, TimingsRearType.Handle> {
  final ConcurrentHashMap<EdgeContext, CompletableFuture<?>> edgeContextDone = new ConcurrentHashMap<>();

  public class Instance implements FlameRuntime.RearInstance<Rear> {
    @Override
    public Class<Rear> clazz() {
      return Rear.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{TimingsRearType.this};
    }
  }

  public static class Rear implements com.spbsu.flamestream.runtime.edge.Rear {
    private final EdgeContext edgeContext;
    private final TimingsRearType type;

    public Rear(EdgeContext edgeContext, TimingsRearType type) {
      this.edgeContext = edgeContext;
      this.type = type;
    }

    Batch last = Batch.Default.EMPTY;

    @Override
    public CompletionStage<?> accept(Batch batch) {
      if (batch.time().time() < Long.MAX_VALUE) {
        final var window = batch.time().time() - 10;
        final var processed = batch.lastGlobalTimeProcessedAt().get(window);
        final var now = Instant.now();
        System.out.println(
                withDigitSeparators(window) + " seconds window timings " + edgeContext.edgeId()
                        + ": processed = " + formattedLatencyNanos(Instant.ofEpochSecond(window), processed)
                        + ", notified = " + formattedLatencyNanos(Instant.ofEpochSecond(window), now)
        );
      }
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

    private static String formattedLatencyNanos(Instant from, Instant to) {
      return NumberFormat.getNumberInstance(Locale.US).format(from.until(to, ChronoUnit.NANOS));
    }

    private static String withDigitSeparators(long nanos) {
      return NumberFormat.getNumberInstance(Locale.US).format(nanos);
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
