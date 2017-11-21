package com.spbsu.flamestream.runtime.node.materializer;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.RemoteScope;
import akka.routing.Router;
import com.spbsu.flamestream.runtime.acker.AckActor;
import com.spbsu.flamestream.runtime.node.api.NewGraph;
import com.spbsu.flamestream.runtime.node.materializer.graph.LocalGraph;
import com.spbsu.flamestream.runtime.utils.HashRange;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;


/**
 * Materializes graph on the current set of worker nodes
 * If this set is changed than the materializer will be changed
 */
public class GraphMaterializer extends LoggingActor {
  private final Map<HashRange, Address> cluster;

  private GraphMaterializer(Map<HashRange, Address> cluster) {
    this.cluster = cluster;
  }

  public static Props props(Map<String, Address> cluster) {
    return Props.create(GraphMaterializer.class, cluster);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(NewGraph.class, this::deploy)
            .build();
  }

  private void deploy(NewGraph graph) {
    final ActorRef acker = context().actorOf(AckActor.props(), "acker_" + graph.id());
    final Map<HashRange, ActorRef> rangeGraphs = new HashMap<>();
    cluster.forEach((range, address) -> {
      final ActorRef rangeGraph = context().actorOf(
              LocalGraph.props(graph.graph()).withDeploy(new Deploy(new RemoteScope(address))),
              range.toString()
      );
      rangeGraphs.put(range, rangeGraph);
    });

    acker.tell(new GraphRoutes(rangeGraphs, acker), self());
    rangeGraphs.values().forEach(g -> g.tell(new GraphRoutes(rangeGraphs, acker), self()));
  }

  private final Router `
}
