package com.spbsu.datastream.core.range.atomic;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Commit;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.range.AtomicCommitDone;

public final class AtomicActor extends LoggingActor {
  private final AtomicGraph atomic;
  private final AtomicHandle handle;

  private AtomicActor(AtomicGraph atomic, AtomicHandle handle) {
    this.atomic = atomic;
    this.handle = handle;
  }

  public static Props props(AtomicGraph atomic, AtomicHandle handle) {
    return Props.create(AtomicActor.class, atomic, handle);
  }

  @Override
  public void preStart() throws Exception {
    this.atomic.onStart(this.handle);
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(PortBindDataItem.class, this::onAddressedMessage)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, this::onCommit)
            .build();
  }

  private void onCommit(Commit commit) {
    this.atomic.onCommit(this.handle);
    this.context().parent().tell(new AtomicCommitDone(this.atomic), ActorRef.noSender());
    this.LOG().info("Commit done");
    this.context().stop(this.self());
  }

  private void onAddressedMessage(PortBindDataItem message) {
    this.atomic.onPush(message.inPort(), message.payload(), this.handle);
    this.handle.ack(message.payload());
  }

  private void onMinTimeUpdate(MinTimeUpdate message) {
    this.atomic.onMinGTimeUpdate(message.minTime(), this.handle);
  }
}
