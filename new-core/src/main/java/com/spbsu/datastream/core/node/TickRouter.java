package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.tick.TickMessage;

import java.util.HashMap;
import java.util.Map;

final class TickRouter extends LoggingActor {
  private final Map<Long, ActorRef> ticks = new HashMap<>();

  public static Props props() {
    return Props.create(TickRouter.class);
  }


  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof TickMessage) {
      final TickMessage<?> tickMessage = (TickMessage<?>) message;
      final ActorRef receiver = this.ticks.getOrDefault(tickMessage.tick(), this.context().system().deadLetters());
      receiver.tell(tickMessage.payload(), this.sender());
    } else if (message instanceof RegisterTick) {
      // TODO: 5/8/17 DEREGISTER TICKS
      final RegisterTick tick = (RegisterTick) message;
      this.ticks.putIfAbsent(tick.tick(), tick.tickConcierge());
    } else {
      this.unhandled(message);
    }
  }

  public static final class RegisterTick {
    private final long tick;

    private final ActorRef tickConcierge;

    public RegisterTick(final long tick, final ActorRef concierge) {
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
