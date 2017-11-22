package com.spbsu.flamestream.runtime.node.materializer;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.RemoteScope;
import com.spbsu.flamestream.runtime.node.api.GraphInstance;
import com.spbsu.flamestream.runtime.node.materializer.acker.Acker;
import com.spbsu.flamestream.runtime.node.materializer.graph.LocalGraph;
import com.spbsu.flamestream.runtime.node.materializer.router.CoarseRouter;
import com.spbsu.flamestream.runtime.node.materializer.router.FlameRouter;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.Map;


/**
 * Materializes graph on the current set of worker nodes
 * If this set is changed than the materializer will be changed
 */
public class GraphMaterializer extends LoggingActor {
  private final Map<IntRange, Address> cluster;

  private GraphMaterializer(Map<IntRange, Address> cluster) {
    this.cluster = cluster;
  }

  public static Props props(Map<IntRange, Address> cluster) {
    return Props.create(GraphMaterializer.class, cluster);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GraphInstance.class, this::deploy)
            .build();
  }

  private void deploy(GraphInstance instance) {
    final ActorRef acker = context().actorOf(Acker.props((frontId, attachTimestamp) -> {}), "acker_" + instance.id());
    final Map<IntRange, ActorRef> rangeGraphs = new HashMap<>();
    cluster.forEach((range, address) -> {
      final ActorRef rangeGraph = context().actorOf(
              LocalGraph.props(instance.graph()).withDeploy(new Deploy(new RemoteScope(address))),
              range.toString()
      );
      rangeGraphs.put(range, rangeGraph);
    });

    final FlameRouter router = new CoarseRouter(instance.graph(), rangeGraphs);
    acker.tell(router, self());
    rangeGraphs.values().forEach(g -> g.tell(router, self()));
  }
}
