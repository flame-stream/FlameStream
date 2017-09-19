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
    this.initializedGraph = this.initializedAtomics(tickInfo.graph().graph().subGraphs());
    this.myRange = myRange;

    // TODO: 5/8/17 akka routing
    this.routingTable = RangeConcierge.withFlattenedKey(this.initializedGraph);
  }

  public static Props props(TickInfo info, ActorRef dns, HashRange myRange,
                            DB db) {
    return Props.create(RangeConcierge.class, info, dns, myRange, db);
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(AtomicMessage.class, this::routeToPort)
            .match(MinTimeUpdate.class, this::broadcast)
            .match(Commit.class, this::handleCommit)
            .build();

  }

  private void handleCommit(Commit commit) {
    this.initializedGraph.values().forEach(atom -> atom.tell(commit, this.sender()));

    this.getContext().become(this.receiveBuilder()
            .match(AtomicCommitDone.class, cd -> this.processCommitDone(cd.graph()))
            .build());
  }

  private void routeToPort(AtomicMessage<?> atomicMessage) {
    final ActorRef route = this.routingTable.getOrDefault(atomicMessage.port(), this.context().system().deadLetters());
    route.tell(atomicMessage, this.sender());
  }

  private void broadcast(Object message) {
    this.routingTable.values().forEach(atomic -> atomic.tell(message, this.sender()));
  }

  private void processCommitDone(AtomicGraph atomicGraph) {
    this.initializedGraph.remove(atomicGraph);
    if (this.initializedGraph.isEmpty()) {
      this.dns.tell(new UnresolvedMessage<>(this.tickInfo.ackerLocation(),
                      new AckerMessage<>(new CommitDone(this.myRange), this.tickInfo.startTs())),
              this.self());
      this.LOG().info("Commit done");
      this.context().stop(this.self());
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
    this.LOG().debug("Creating actor for atomic: id= {}, class={}", id, atomic.getClass());
    return this.context().actorOf(AtomicActor.props(atomic, this.tickInfo, this.dns, this.db), id);
  }
}
