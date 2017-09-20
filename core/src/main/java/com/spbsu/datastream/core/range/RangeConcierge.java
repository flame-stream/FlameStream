package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Commit;
import com.spbsu.datastream.core.ack.CommitDone;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.message.AckerMessage;
import com.spbsu.datastream.core.message.AtomicMessage;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.range.atomic.AtomicActor;
import com.spbsu.datastream.core.tick.StartTick;
import com.spbsu.datastream.core.tick.TickInfo;
import com.spbsu.datastream.core.tick.RoutingInfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

public final class RangeConcierge extends LoggingActor {
  private final TickInfo tickInfo;
  private final HashRange range;

  private Map<InPort, ActorRef> routingTable;
  private Map<AtomicGraph, ActorRef> initializedGraph;
  private RoutingInfo routingInfo;

  private RangeConcierge(TickInfo tickInfo, HashRange range) {
    this.tickInfo = tickInfo;
    this.range = range;

  }

  public static Props props(TickInfo info, HashRange range) {
    return Props.create(RangeConcierge.class, info, range);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(StartTick.class, startTick -> {
              routingInfo = startTick.tickRoutingInfo();
              initializedGraph = initializedAtomics(tickInfo.graph().graph().subGraphs(), routingInfo);
              routingTable = RangeConcierge.withFlattenedKey(initializedGraph);

              getContext().become(ranging());
            })
            .build();
  }

  public Receive ranging() {
    return receiveBuilder()
            .match(AddressedItem.class, this::routeToPort)
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

  private void routeToPort(AddressedItem atomicMessage) {
    final ActorRef route = routingTable.getOrDefault(atomicMessage.port(), context().system().deadLetters());
    route.tell(atomicMessage, sender());
  }

  private void broadcast(Object message) {
    routingTable.values().forEach(atomic -> atomic.tell(message, sender()));
  }

  private void processCommitDone(AtomicGraph atomicGraph) {
    initializedGraph.remove(atomicGraph);
    if (initializedGraph.isEmpty()) {
      routingInfo.acker().tell(new CommitDone(range), self());
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

  private Map<AtomicGraph, ActorRef> initializedAtomics(Collection<? extends AtomicGraph> atomicGraphs, RoutingInfo routingInfo) {
    return atomicGraphs.stream()
            .collect(toMap(Function.identity(), atomic -> actorForAtomic(atomic, routingInfo)));
  }

  private ActorRef actorForAtomic(AtomicGraph atomic, RoutingInfo routingInfo) {
    final String id = UUID.randomUUID().toString();
    LOG().debug("Creating actor for atomic: id= {}, class={}", id, atomic.getClass());
    return context().actorOf(AtomicActor.props(atomic, tickInfo, routingInfo), id);
  }
}
