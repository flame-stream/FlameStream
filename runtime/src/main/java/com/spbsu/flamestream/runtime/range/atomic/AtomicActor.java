package com.spbsu.flamestream.runtime.range.atomic;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.runtime.ack.Commit;
import com.spbsu.flamestream.runtime.ack.MinTimeUpdate;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.range.AddressedItem;
import com.spbsu.flamestream.runtime.range.AtomicCommitDone;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;

public final class AtomicActor extends LoggingActor {
  private final AtomicActorStatistics stat = new AtomicActorStatistics();
  private final AtomicGraph atomic;
  private final AtomicHandle handle;

  private AtomicActor(AtomicGraph atomic, TickInfo tickInfo, TickRoutes tickRoutes) {
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
    handle.ack(item.ack(), item.meta().globalTime());

    final long stop = System.nanoTime();
    stat.recordOnAtomicMessage(stop - start);
  }

  private void onMinTimeUpdate(MinTimeUpdate message) {
    final long start = System.nanoTime();

    atomic.onMinGTimeUpdate(message.minTime(), handle);

    final long stop = System.nanoTime();
    stat.recordOnMinTimeUpdate(stop - start);
  }
}
