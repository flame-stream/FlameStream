package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.graph.materialization.ActorPerNodeGraphMaterializer;
import com.spbsu.flamestream.runtime.graph.materialization.BarrierRouter;
import com.spbsu.flamestream.runtime.graph.materialization.FlameRouter;
import com.spbsu.flamestream.runtime.graph.materialization.GraphMaterialization;
import com.spbsu.flamestream.runtime.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.Map;

public class GraphManager extends LoggingActor {
  private final GraphMaterialization materialization;

  private GraphManager(Graph logicalGraph, ActorRef acker, ClusterConfig config) {
    materialization = new ActorPerNodeGraphMaterializer(
            acker,
            resolvedRouter(config),
            resolvedBarriers(config)
    ).materialize(logicalGraph);
  }

  public static Props props(Graph logicalGraph, ActorRef acker, ClusterConfig config) {
    return Props.create(GraphManager.class, logicalGraph, acker, config);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, materialization::inject)
            .match(DataItem.class, materialization::accept)
            .build();
  }

  @Override
  public void postStop() {
    materialization.close();
  }

  private BarrierRouter resolvedBarriers(ClusterConfig config) {
    final Map<String, ActorRef> barriers = new HashMap<>();
    config.nodes().forEach(nodeConfig -> {
      final ActorRef b = AwaitResolver.syncResolve(nodeConfig.nodePath().child("barrier"), context());
      barriers.put(nodeConfig.id(), b);
    });
    return (item, sender) -> barriers.get(item.meta().globalTime().front()).tell(item, sender);
  }

  private FlameRouter resolvedRouter(ClusterConfig config) {
    final Map<IntRange, ActorRef> managers = new HashMap<>();
    config.nodes().forEach(nodeConfig -> {
      final ActorRef manager = AwaitResolver.syncResolve(nodeConfig.nodePath().child("graph"), context());
      managers.put(nodeConfig.range().asRange(), manager);
    });
    return (item, sender) -> {

    };
  }
}
