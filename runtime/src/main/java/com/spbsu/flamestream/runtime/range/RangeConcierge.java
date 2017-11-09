package com.spbsu.flamestream.runtime.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.runtime.ack.messages.Commit;
import com.spbsu.flamestream.runtime.ack.messages.MinTimeUpdate;
import com.spbsu.flamestream.runtime.ack.messages.RangeCommitDone;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.range.atomic.AtomicActor;
import com.spbsu.flamestream.runtime.tick.StartTick;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public final class RangeConcierge extends LoggingActor {
  private final TickInfo tickInfo;
  private final HashRange range;

  private Map<InPort, ActorRef> routingTable = null;
  private Map<AtomicGraph, ActorRef> initializedGraph = null;
  private TickRoutes tickRoutes = null;

  private RangeConcierge(TickInfo tickInfo, HashRange range) {
    this.tickInfo = tickInfo;
    this.range = range;

  }

  public static Props props(TickInfo info, HashRange range) {
    return Props.create(RangeConcierge.class, info, range);
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

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create().match(StartTick.class, startTick -> {
      tickRoutes = startTick.tickRoutingInfo();
      initializedGraph = initializedAtomics(tickInfo.graph().graph().subGraphs(), tickRoutes);
      routingTable = RangeConcierge.withFlattenedKey(initializedGraph);

      unstashAll();
      getContext().become(ranging());
    }).matchAny(m -> stash()).build();
  }

  private Receive ranging() {
    return receiveBuilder().match(AddressedItem.class, this::routeToPort)
            .match(MinTimeUpdate.class, this::broadcast)
            .match(Commit.class, this::handleCommit)
            .build();

  }

  private void broadcast(Object message) {
    routingTable.values().forEach(atomic -> atomic.tell(message, sender()));
  }

  private void routeToPort(AddressedItem atomicMessage) {
    final ActorRef route = routingTable.getOrDefault(atomicMessage.port(), context().system().deadLetters());
    route.tell(atomicMessage, sender());
  }

  private void handleCommit(Commit commit) {
    initializedGraph.values().forEach(atom -> atom.tell(commit, sender()));

    getContext().become(ReceiveBuilder.create()
            .match(AtomicCommitDone.class, cd -> processCommitDone(cd.graph()))
            .build());
  }

  private void processCommitDone(AtomicGraph atomicGraph) {
    initializedGraph.remove(atomicGraph);

    if (initializedGraph.isEmpty()) {
      tickRoutes.acker().tell(new RangeCommitDone(range), self());
      log().info("Range commit done");
      context().stop(self());
    }
  }

  private Map<AtomicGraph, ActorRef> initializedAtomics(Collection<? extends AtomicGraph> atomicGraphs,
          TickRoutes tickRoutes) {
    return atomicGraphs.stream().collect(toMap(Function.identity(), atomic -> actorForAtomic(atomic, tickRoutes)));
  }

  private ActorRef actorForAtomic(AtomicGraph atomic, TickRoutes tickRoutes) {
    final String id = UUID.randomUUID().toString();
    log().debug("Creating actor for atomic: id= {}, class={}", id, atomic.getClass());
    return context().actorOf(AtomicActor.props(atomic, tickInfo, tickRoutes), id);
  }
}
