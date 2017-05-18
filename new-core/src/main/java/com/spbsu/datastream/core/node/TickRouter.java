package com.spbsu.datastream.core.node;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.tick.TickMessage;
import org.omg.CORBA.TIMEOUT;

import java.util.HashMap;
import java.util.Map;

final class TickRouter extends LoggingActor {
  private final Map<Long, ActorRef> ticks = new HashMap<>();

  public static Props props() {
    return Props.create(TickRouter.class);
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(TickMessage.class, this::handleTickMessage)
            .match(RegisterTick.class, this::registerTick)
            .build();
  }

  private void registerTick(RegisterTick tick) {
    this.ticks.putIfAbsent(tick.tick(), tick.tickConcierge());
  }

  private void handleTickMessage(TickMessage<?> tickMessage) {
    final ActorRef receiver = this.ticks.getOrDefault(tickMessage.tick(), this.context().system().deadLetters());
    receiver.tell(tickMessage.payload(), this.sender());
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
