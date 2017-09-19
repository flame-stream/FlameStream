package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.message.Message;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

final class TickRouter extends LoggingActor {
  private final TLongObjectMap<ActorRef> ticks = new TLongObjectHashMap<>();

  public static Props props() {
    return Props.create(TickRouter.class);
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(Message.class, this::handleTickMessage)
            .match(RegisterTick.class, this::registerTick)
            .build();
  }

  private void registerTick(RegisterTick tick) {
    this.ticks.putIfAbsent(tick.tick(), tick.tickConcierge());
  }

  private void handleTickMessage(Message<?> tickMessage) {
    final ActorRef receiver = this.ticks.get(tickMessage.tick());
    if (receiver != null) {
      receiver.tell(tickMessage, this.sender());
    } else {
      this.LOG().error("Unknown tick {}", tickMessage.tick());
    }
  }

  public static final class RegisterTick {
    private final long tick;

    private final ActorRef tickConcierge;

    public RegisterTick(long tick, ActorRef concierge) {
      this.tick = tick;
      this.tickConcierge = concierge;
    }

    public ActorRef tickConcierge() {
      return this.tickConcierge;
    }

    public long tick() {
      return this.tick;
    }
  }
}
