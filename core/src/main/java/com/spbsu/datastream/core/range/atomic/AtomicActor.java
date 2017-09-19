package com.spbsu.datastream.core.range.atomic;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.message.AtomicMessage;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Commit;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.range.AtomicCommitDone;
import com.spbsu.datastream.core.stat.AtomicActorStatistics;
import com.spbsu.datastream.core.tick.TickInfo;
import org.iq80.leveldb.DB;

public final class AtomicActor extends LoggingActor {
  private final AtomicActorStatistics stat = new AtomicActorStatistics();
  private final AtomicGraph atomic;
  private final AtomicHandle handle;

  private AtomicActor(AtomicGraph atomic, TickInfo tickInfo, ActorRef dns, DB db) {
    this.atomic = atomic;
    this.handle = new AtomicHandleImpl(tickInfo, dns, db, this.context());
  }

  public static Props props(AtomicGraph atomic, TickInfo tickInfo, ActorRef dns, DB db) {
    return Props.create(AtomicActor.class, atomic, tickInfo, dns, db);
  }

  @Override
  public void preStart() throws Exception {
    this.atomic.onStart(this.handle);
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(AtomicMessage.class, this::onAtomicMessage)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, this::onCommit)
            .build();
  }

  private void onCommit(Commit commit) {
    this.atomic.onCommit(this.handle);
    this.context().parent().tell(new AtomicCommitDone(this.atomic), this.self());
    this.LOG().info("Commit done");
    this.context().stop(this.self());

    this.LOG().info("Atomic {} statistics: {}", atomic, stat);
  }

  @Override
  public void postStop() throws Exception {
    this.LOG().info("Atomic {} statistics: {}", atomic, stat);

    super.postStop();
  }

  private void onAtomicMessage(AtomicMessage<?> message) {
    final long start = System.nanoTime();

    this.atomic.onPush(message.port(), message.payload(), this.handle);
    this.handle.ack(message.payload());

    final long stop = System.nanoTime();
    stat.recordOnAtomicMessage(stop - start);
  }

  private void onMinTimeUpdate(MinTimeUpdate message) {
    final long start = System.nanoTime();

    this.atomic.onMinGTimeUpdate(message.minTime(), this.handle);

    final long stop = System.nanoTime();
    stat.recordOnMinTimeUpdate(stop - start);
  }
}
