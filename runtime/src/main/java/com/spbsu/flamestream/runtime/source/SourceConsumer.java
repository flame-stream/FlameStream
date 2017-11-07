package com.spbsu.flamestream.runtime.source;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.runtime.actor.LoggingActor;

final class SourceConsumer extends LoggingActor {
  private final AtomicHandle handle;
  private final OutPort outPort;
  private ActorRef front;
  private int capacity = 100;

  private SourceConsumer(AtomicHandle handle, OutPort outPort) {
    this.handle = handle;
    this.outPort = outPort;

    resolveFront();
  }

  @Override
  public void preStart() {
    super.aroundPreStart();
    front.tell(new Start(), self());
  }

  private void resolveFront() {
  }

  public static Props props(AtomicHandle handle, OutPort outPort) {
    return Props.create(SourceConsumer.class, handle, outPort);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, i -> {
              handle.push(outPort, i);
              capacity--;
              if (capacity < 0) {
                front.tell(new PullBased(), self());
              }
            })
            .match(Exception.class, e -> handle.error("Smth went wrong {}", e))
            .match(GlobalTime.class, e -> {
              capacity++;
              front.tell(new RequestMore(1), self());
            })
            .build();
  }
}
