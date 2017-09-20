package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.message.AckerMessage;
import com.spbsu.datastream.core.message.AtomicMessage;
import com.spbsu.datastream.core.message.BroadcastMessage;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.message.Message;
import com.spbsu.datastream.core.ack.AckActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.iq80.leveldb.DB;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class TickConcierge extends LoggingActor {
  private final TickInfo info;
  private final ActorRef dns;
  private final int localId;
  private final DB db;

  private final TreeMap<Integer, ActorRef> concierges;

  private final ActorRef acker;

  private TickConcierge(TickInfo tickInfo,
                        int localId,
                        ActorRef dns, DB db) {
    this.info = tickInfo;
    this.dns = dns;
    this.localId = localId;

    this.concierges = new TreeMap<>();
    myRanges(tickInfo.hashMapping().asMap())
            .forEach(range -> concierges.put(range.from(), rangeConcierge(range)));
    this.db = db;
    if (tickInfo.ackerLocation() == localId) {
      this.acker = context().actorOf(AckActor.props(tickInfo, dns), "acker");
    } else {
      this.acker = null;
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(AckerMessage.class, m -> acker.tell(m.payload(), sender()))
            .match(AtomicMessage.class, this::routeAtomicMessage)
            .match(BroadcastMessage.class, this::broadcast)
            .build();
  }

  private ActorRef rangeConcierge(HashRange range) {
    return context().actorOf(RangeConcierge.props(info, dns, range, db), range.toString());
  }

  public static Props props(TickInfo tickInfo, DB db, int localId,
                            ActorRef dns) {
    return Props.create(TickConcierge.class, tickInfo, localId, dns, db);
  }

  private Iterable<HashRange> myRanges(Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(localId))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  private void broadcast(Message<?> broadcastMessage) {
    concierges.values().forEach(v -> v.tell(broadcastMessage.payload(), sender()));
  }

  private void routeAtomicMessage(AtomicMessage<?> atomicMessage) {
    final ActorRef receiver = concierges
            .floorEntry(atomicMessage.hash()).getValue();
    receiver.tell(atomicMessage, sender());
  }
}
