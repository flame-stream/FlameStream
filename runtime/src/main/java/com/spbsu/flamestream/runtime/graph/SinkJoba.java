package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.edge.api.GimmeLastBatch;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SinkJoba implements Joba {
  private final LoggingAdapter log;
  private final InvalidatingBucket invalidatingBucket = new ArrayInvalidatingBucket();
  private final List<ActorRef> rears = new ArrayList<>();
  private final ActorContext context;

  private final Tracing.Tracer barrierReceiveTracer = Tracing.TRACING.forEvent("barrier-receive");
  private final Tracing.Tracer barrierSendTracer = Tracing.TRACING.forEvent("barrier-send");

  private GlobalTime lastEmitted = GlobalTime.MIN;

  public SinkJoba(ActorContext context) {
    log = Logging.getLogger(context.system(), context.self());
    this.context = context;
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    barrierReceiveTracer.log(item.xor());
    invalidatingBucket.insert(item);
  }

  public void attachRear(ActorRef rear) {
    rears.add(rear);
    try {
      final Batch batch = PatternsCS.ask(rear, new GimmeLastBatch(), TimeUnit.SECONDS.toMillis(10))
              .thenApply(e -> (Batch) e)
              .toCompletableFuture().get();
      if (batch == Batch.EMPTY_BATCH) {
        this.lastEmitted = GlobalTime.MIN;
      } else {
        this.lastEmitted = ((BatchImpl) batch).time();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    if (lastEmitted.compareTo(minTime) < 0) {
      final int pos = invalidatingBucket.lowerBound(new Meta(minTime));

      final List<DataItem> data = new ArrayList<>();
      invalidatingBucket.forRange(0, pos, data::add);

      if (!data.isEmpty()) {
        if (rears.size() != 1) {
          throw new IllegalStateException("There should be exactly one rear:)");
        }

        final BatchImpl batch = new BatchImpl(minTime, data);

        try {
          PatternsCS.ask(rears.get(0), batch, TimeUnit.SECONDS.toMillis(10)).toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }

        barrierSendTracer.log(batch.time().time());
        invalidatingBucket.clearRange(0, pos);
      }
    } else {
      log.warning("Barrier has already emitted for the time '{}'", minTime);
    }
  }

  private static class BatchImpl implements Batch {
    private final List<DataItem> items;
    private final GlobalTime time;

    private BatchImpl(GlobalTime time, List<DataItem> items) {
      this.items = items;
      this.time = time;
    }

    public GlobalTime time() {
      return time;
    }

    @Override
    public <T> Stream<T> payload(Class<T> clazz) {
      return items.stream().map(i -> i.payload(clazz));
    }
  }
}
