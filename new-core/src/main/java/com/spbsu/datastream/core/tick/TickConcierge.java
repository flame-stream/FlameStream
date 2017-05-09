package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.ack.AckActor;
import com.spbsu.datastream.core.ack.CommitDone;
import com.spbsu.datastream.core.ack.FrontReport;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.HashedMessage;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.iq80.leveldb.DB;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TickConcierge extends LoggingActor {
  private final TickInfo info;
  private final ActorRef dns;
  private final int localId;
  private final DB db;

  private final Map<HashRange, ActorRef> concierges;

  @Nullable
  private final ActorRef acker;

  private TickConcierge(final TickInfo tickInfo,
                        final int localId,
                        final ActorRef dns, final DB db) {
    this.info = tickInfo;
    this.dns = dns;
    this.localId = localId;

    this.concierges = this.myRanges(tickInfo.hashMapping()).stream()
            .collect(Collectors.toMap(Function.identity(), this::rangeConcierge));
    this.db = db;
    if (tickInfo.ackerLocation() == localId) {
      this.acker = this.context().actorOf(AckActor.props(tickInfo, dns), "acker");
    } else {
      this.acker = null;
    }
  }

  private ActorRef rangeConcierge(final HashRange range) {
    return this.context().actorOf(RangeConcierge.props(this.info, this.dns, range, db), range.toString());
  }

  public static Props props(final TickInfo tickInfo, final DB db, final int localId,
                            final ActorRef dns) {
    return Props.create(TickConcierge.class, tickInfo, localId, dns, db);
  }

  private Collection<HashRange> myRanges(final Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(this.localId))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }


  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received {}", message);
    if (message instanceof HashedMessage) {
      final HashedMessage<?> hashedMessage = (HashedMessage<?>) message;
      this.routeHashedMessage(hashedMessage);
    } else if (message instanceof FrontReport || message instanceof Ack || message instanceof CommitDone) {
      this.acker.tell(message, this.sender());
    } else {
      this.unhandled(message);
    }
  }

  // TODO: 5/9/17 do it with akka routing
  private void routeHashedMessage(final HashedMessage<?> hashedMessage) {
    if (hashedMessage.isBroadcast()) {
      this.concierges.values().forEach(v -> v.tell(hashedMessage.payload(), this.sender()));
    } else {
      final ActorRef receiver = this.concierges.entrySet().stream().filter(e -> e.getKey().contains(hashedMessage.hash()))
              .map(Map.Entry::getValue).findAny().orElse(this.context().system().deadLetters());
      receiver.tell(hashedMessage.payload(), this.sender());
    }
  }
}
