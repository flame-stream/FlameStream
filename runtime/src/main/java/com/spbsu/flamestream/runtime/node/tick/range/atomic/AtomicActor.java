package com.spbsu.flamestream.runtime.node.tick.range.atomic;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.utils.Statistics;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.runtime.acker.api.Commit;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.spbsu.flamestream.runtime.node.tick.api.TickRoutes;
import com.spbsu.flamestream.runtime.node.tick.range.api.AddressedItem;
import com.spbsu.flamestream.runtime.node.tick.range.api.AtomicCommitDone;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

public class AtomicActor extends LoggingActor {
  private final AtomicActorStatistics stat = new AtomicActorStatistics();
  private final AtomicGraph atomic;
  private final AtomicHandle handle;

  protected AtomicActor(AtomicGraph atomic, TickInfo tickInfo, TickRoutes tickRoutes) {
    this.atomic = atomic;
    this.handle = new AtomicHandleImpl(tickInfo, tickRoutes, context());
  }

  public static Props props(AtomicGraph atomic, TickInfo tickInfo, TickRoutes tickRoutes) {
    return Props.create(AtomicActor.class, atomic, tickInfo, tickRoutes);
  }

  @Override
  public void preStart() throws Exception {
    atomic.onStart(handle);
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::onAtomicMessage)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, commit -> onCommit())
            .build();
  }

  private void onCommit() {
    atomic.onCommit(handle);
    context().parent().tell(new AtomicCommitDone(atomic), self());
    log().info("Commit done");

    context().stop(self());
  }

  @Override
  public void postStop() {
    log().info("Atomic {} statistics: {}", atomic, stat);
    super.postStop();
  }

  private void onAtomicMessage(AddressedItem message) {
    final long start = System.nanoTime();

    final DataItem<?> item = message.item();
    atomic.onPush(message.port(), item, handle);
    handle.ack(item.xor(), item.meta().globalTime());

    final long stop = System.nanoTime();
    stat.recordOnAtomicMessage(stop - start);
  }

  protected void onMinTimeUpdate(MinTimeUpdate message) {
    final long start = System.nanoTime();

    atomic.onMinGTimeUpdate(message.minTime(), handle);

    final long stop = System.nanoTime();
    stat.recordOnMinTimeUpdate(stop - start);
  }

  private static class AtomicActorStatistics implements Statistics {
    private final LongSummaryStatistics onAtomic = new LongSummaryStatistics();
    private final LongSummaryStatistics onMinTime = new LongSummaryStatistics();

    void recordOnAtomicMessage(long nanoDuration) {
      onAtomic.accept(nanoDuration);
    }

    void recordOnMinTimeUpdate(long nanoDuration) {
      onMinTime.accept(nanoDuration);
    }

    @Override
    public Map<String, Double> metrics() {
      final Map<String, Double> result = new HashMap<>();
      result.putAll(Statistics.asMap("onAtomicMessage duration", onAtomic));
      result.putAll(Statistics.asMap("onMinTime duration", onMinTime));
      return result;
    }

    @Override
    public String toString() {
      return metrics().toString();
    }
  }
}
