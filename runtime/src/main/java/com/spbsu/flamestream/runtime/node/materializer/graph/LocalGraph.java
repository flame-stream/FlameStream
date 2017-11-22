package com.spbsu.flamestream.runtime.node.materializer.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.source.Source;
import com.spbsu.flamestream.runtime.node.materializer.AddressedItem;
import com.spbsu.flamestream.runtime.node.materializer.GraphRoutes;
import com.spbsu.flamestream.runtime.node.materializer.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.node.materializer.graph.api.Commit;
import com.spbsu.flamestream.runtime.node.materializer.graph.api.MaterializationCommitDone;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.AtomicActor;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.api.AtomicCommitDone;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.SourceActor;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class LocalGraph extends LoggingActor {
  private final ComposedGraph<AtomicGraph> graph;

  private Map<InPort, ActorRef> routingTable = null;
  private Map<AtomicGraph, ActorRef> initializedGraph = null;
  private GraphRoutes routes = null;

  private LocalGraph(ComposedGraph<AtomicGraph> graph) {
    this.graph = graph;
  }

  public static Props props(ComposedGraph<AtomicGraph> graph) {
    return Props.create(LocalGraph.class, graph);
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
    return ReceiveBuilder.create()
            .match(GraphRoutes.class, routes -> {
              initializedGraph = initializedAtomics(graph.flattened().subGraphs(), routes);
              routingTable = LocalGraph.withFlattenedKey(initializedGraph);
              context().actorSelection("/user/watcher/node/front_barrier").tell(routingTable, self());
              unstashAll();
              getContext().become(ranging());
            })
            .matchAny(m -> stash()).build();
  }

  private Receive ranging() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::routeToPort)
            .match(MinTimeUpdate.class, this::broadcast)
            .match(Commit.class, this::handleCommit)
            .build();
  }

  private void broadcast(Object message) {
    initializedGraph.values().forEach(atomic -> atomic.tell(message, sender()));
  }

  private void routeToPort(AddressedItem atomicMessage) {
    final ActorRef route = routingTable.getOrDefault(atomicMessage.destanation(), context().system().deadLetters());
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
      routes.acker().tell(new MaterializationCommitDone(), self());
      log().info("Mat commit done");
      context().stop(self());
    }
  }

  private Map<AtomicGraph, ActorRef> initializedAtomics(Collection<? extends AtomicGraph> atomicGraphs,
                                                        GraphRoutes tickRoutes) {
    return atomicGraphs.stream().collect(toMap(Function.identity(), atomic -> actorForAtomic(atomic, tickRoutes)));
  }

  private ActorRef actorForAtomic(AtomicGraph atomic, GraphRoutes tickRoutes) {
    final String id = UUID.randomUUID().toString();
    log().debug("Creating actor for atomic: frontId= {}, class={}", id, atomic.getClass());
    if (atomic instanceof Source) {
      return context().actorOf(SourceActor.props(atomic, tickRoutes), id);
    } else {
      return context().actorOf(AtomicActor.props(atomic, tickRoutes), id);
    }
  }
}
