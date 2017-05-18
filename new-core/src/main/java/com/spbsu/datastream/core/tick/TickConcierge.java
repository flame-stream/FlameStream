package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.AckerMessage;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.ack.AckActor;
import com.spbsu.datastream.core.ack.AckerReport;
import com.spbsu.datastream.core.ack.CommitDone;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.HashedMessage;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.iq80.leveldb.DB;

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

  private final ActorRef acker;

  private TickConcierge(TickInfo tickInfo,
                        int localId,
                        ActorRef dns, DB db) {
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

  @Override
  public Receive createReceive() {
    return this.receiveBuilder().match(HashedMessage.class, this::routeHashedMessage)
            .match(AckerMessage.class, m -> this.acker.forward(m, this.getContext()))
            .build();
  }

  private ActorRef rangeConcierge(HashRange range) {
    return this.context().actorOf(RangeConcierge.props(this.info, this.dns, range, db), range.toString());
  }

  public static Props props(TickInfo tickInfo, DB db, int localId,
                            ActorRef dns) {
    return Props.create(TickConcierge.class, tickInfo, localId, dns, db);
  }

  private Collection<HashRange> myRanges(Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(this.localId))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  // TODO: 5/9/17 do it with akka routing
  private void routeHashedMessage(HashedMessage<?> hashedMessage) {
    if (hashedMessage.isBroadcast()) {
      this.concierges.values().forEach(v -> v.tell(hashedMessage.payload(), this.sender()));
    } else {
      final ActorRef receiver = this.concierges.entrySet().stream().filter(e -> e.getKey().contains(hashedMessage.hash()))
              .map(Map.Entry::getValue).findAny().orElse(this.context().system().deadLetters());
      receiver.tell(hashedMessage.payload(), this.sender());
    }
  }
}
