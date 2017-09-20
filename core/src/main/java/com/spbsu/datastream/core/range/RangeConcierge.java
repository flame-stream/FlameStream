package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.message.AckerMessage;
import com.spbsu.datastream.core.message.AtomicMessage;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Commit;
import com.spbsu.datastream.core.ack.CommitDone;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.range.atomic.AtomicActor;
import com.spbsu.datastream.core.tick.TickInfo;
import org.iq80.leveldb.DB;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class RangeConcierge extends LoggingActor {
  private final TickInfo tickInfo;
  private final ActorRef dns;
  private final HashRange myRange;

  private final Map<AtomicGraph, ActorRef> initializedGraph;
  private final Map<InPort, ActorRef> routingTable;
  private final DB db;

  private RangeConcierge(TickInfo tickInfo,
                         ActorRef dns,
                         HashRange myRange,
                         DB db) {
    this.tickInfo = tickInfo;
    this.db = db;
    this.dns = dns;
    this.initializedGraph = initializedAtomics(tickInfo.graph().graph().subGraphs());
    this.myRange = myRange;

    // TODO: 5/8/17 akka routing
    this.routingTable = RangeConcierge.withFlattenedKey(initializedGraph);
  }

  public static Props props(TickInfo info, ActorRef dns, HashRange myRange,
                            DB db) {
    return Props.create(RangeConcierge.class, info, dns, myRange, db);
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(AtomicMessage.class, this::routeToPort)
            .match(MinTimeUpdate.class, this::broadcast)
            .match(Commit.class, this::handleCommit)
            .build();

  }

  private void handleCommit(Commit commit) {
    initializedGraph.values().forEach(atom -> atom.tell(commit, sender()));

    getContext().become(receiveBuilder()
            .match(AtomicCommitDone.class, cd -> processCommitDone(cd.graph()))
            .build());
  }

  private void routeToPort(AtomicMessage<?> atomicMessage) {
    final ActorRef route = routingTable.getOrDefault(atomicMessage.port(), context().system().deadLetters());
    route.tell(atomicMessage, sender());
  }

  private void broadcast(Object message) {
    routingTable.values().forEach(atomic -> atomic.tell(message, sender()));
  }

  private void processCommitDone(AtomicGraph atomicGraph) {
    initializedGraph.remove(atomicGraph);
    if (initializedGraph.isEmpty()) {
      dns.tell(new UnresolvedMessage<>(tickInfo.ackerLocation(),
                      new AckerMessage<>(new CommitDone(myRange), tickInfo.startTs())),
              self());
      LOG().info("Commit done");
      context().stop(self());
    }
  }

  private static Map<InPort, ActorRef> withFlattenedKey(Map<AtomicGraph, ActorRef> map) {
    final Map<InPort, ActorRef> result = new HashMap<>();
    for (Map.Entry<AtomicGraph, ActorRef> e : map.entrySet()) {
      for (InPort port : e.getKey().inPorts()) {
        result.put(port, e.getValue());
      }
    }
    return result;
  }

  private Map<AtomicGraph, ActorRef> initializedAtomics(Collection<? extends AtomicGraph> atomicGraphs) {
    return atomicGraphs.stream().collect(Collectors.toMap(Function.identity(), this::actorForAtomic));
  }

  private ActorRef actorForAtomic(AtomicGraph atomic) {
    final String id = UUID.randomUUID().toString();
    LOG().debug("Creating actor for atomic: id= {}, class={}", id, atomic.getClass());
    return context().actorOf(AtomicActor.props(atomic, tickInfo, dns, db), id);
  }
}
