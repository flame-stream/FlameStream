package com.spbsu.datastream.core.barrier;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.front.RawData;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

public final class RemoteActorConsumer<T> extends BarrierSink<T> {
  private final ActorPath path;

  private ActorSelection actor;

  public RemoteActorConsumer(ActorPath path) {
    this.path = path;
  }

  @Override
  public void onStart(AtomicHandle handle) {
    this.actor = handle.actorSelection(this.path);
  }

  @Override
  protected void consume(T payload) {
    this.actor.tell(new RawData<>(payload), ActorRef.noSender());
  }
}

