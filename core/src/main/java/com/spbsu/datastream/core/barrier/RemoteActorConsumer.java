package com.spbsu.datastream.core.barrier;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.front.RawData;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;

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

