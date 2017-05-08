package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.ack.AckActor;
import com.spbsu.datastream.core.ack.FrontReport;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.HashedMessage;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TickConcierge extends LoggingActor {
  private final TickInfo info;

  private final ActorRef dns;
  private final int localId;

  private final Map<HashRange, ActorRef> concierges;

  @Nullable
  private final ActorRef acker;

  private TickConcierge(final TickInfo tickInfo,
                        final int localId,
                        final ActorRef dns) {
    this.info = tickInfo;
    this.dns = dns;
    this.localId = localId;

    this.concierges = this.myRanges(tickInfo.hashMapping()).stream()
            .collect(Collectors.toMap(Function.identity(), this::rangeConcierge));
    if (tickInfo.ackerLocation() == localId) {
      this.acker = this.context().actorOf(AckActor.props(tickInfo, dns), "acker");
    } else {
      this.acker = null;
    }
  }

  private ActorRef rangeConcierge(final HashRange range) {
    return this.context().actorOf(RangeConcierge.props(this.info, this.dns), range.toString());
  }

  public static Props props(final TickInfo tickInfo, final int localId, final ActorRef dns) {
    return Props.create(TickConcierge.class, tickInfo, localId, dns);
  }

  private Collection<HashRange> myRanges(final Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(this.localId))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }


  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received {}" ,message);
    if (message instanceof HashedMessage) {
      final HashedMessage<?> hashedMessage = (HashedMessage<?>) message;
      final ActorRef receiver = this.concierges.entrySet().stream().filter(e -> e.getKey().contains(hashedMessage.hash()))
              .map(Map.Entry::getValue).findAny().orElse(this.context().system().deadLetters());
      receiver.tell(hashedMessage.payload(), this.sender());
    } else if (message instanceof FrontReport || message instanceof Ack) {
      this.acker.tell(message, this.sender());
    } else if (message instanceof MinTimeUpdate) {
      this.concierges.values().forEach(v -> v.tell(message, this.sender()));
    } else {
      this.unhandled(message);
    }
  }
}
