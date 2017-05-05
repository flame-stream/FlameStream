package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import com.spbsu.datastream.core.LoggingActor;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.spbsu.datastream.core.range.RangeRouterApi.RegisterMe;

final class RangeRouter extends LoggingActor {
  private final Map<Long, ActorRef> tickLocalRouters = new HashMap<>();

  private RangeRouter() {
  }

  public static Props props() {
    return Props.create(RangeRouter.class);
  }

  @SuppressWarnings({"IfStatementWithTooManyBranches", "ChainOfInstanceofChecks"})
  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      this.route((AddressedMessage<?>) message);
    } else if (message instanceof RegisterMe) {
      this.register((RegisterMe) message);
    } else if (message instanceof Terminated) {
      this.unregister((Terminated) message);
    } else {
      this.unhandled(message);
    }
  }

  private void route(final AddressedMessage<?> message) {
    this.LOG().debug("Routing of {}", message);

    final long tick = message.tick();
    final ActorRef dest = this.tickLocalRouters.getOrDefault(tick, this.context().system().deadLetters());
    dest.tell(message.payload(), ActorRef.noSender());
  }

  private void register(final RegisterMe registerMe) {
    this.tickLocalRouters.put(registerMe.tick(), registerMe.actorRef());
    this.context().watch(registerMe.actorRef());

    this.LOG().info("Tick local router has been registered. Tick: {}, actor: {}", registerMe.tick(), registerMe.actorRef());
  }

  private void unregister(final Terminated terminated) {
    final long tick = this.tickLocalRouters.entrySet().stream()
            .filter(e -> e.getValue().equals(terminated.actor()))
            .map(Map.Entry::getKey)
            .findAny()
            .orElseThrow(NoSuchElementException::new);

    this.tickLocalRouters.remove(tick);
    this.LOG().info("Tick local router has been unregistered. Tick: {}, actor: {}", tick, terminated.actor());
  }
}
