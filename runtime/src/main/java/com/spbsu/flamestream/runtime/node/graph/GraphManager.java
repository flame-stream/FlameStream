package com.spbsu.flamestream.runtime.node.graph;

import akka.actor.ActorRef;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.RemoteScope;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.graph.acker.Acker;
import com.spbsu.flamestream.runtime.node.graph.api.GraphInstance;
import com.spbsu.flamestream.runtime.node.graph.materialization.GraphMaterialization;
import com.spbsu.flamestream.runtime.node.graph.materialization.GraphRouter;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;


/**
 * Materializes graph on the current set of worker nodes
 * If this set is changed than the graph will be changed
 */
public class GraphManager extends LoggingActor {
  private final Graph<?, ?> logicalGraph;

  private GraphManager(Graph<?, ?> logicalGraph) {
    this.logicalGraph = logicalGraph;
  }

  public static Props props(Graph<?, ?> logicalGraph) {
    return Props.create(GraphManager.class, logicalGraph);
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
              GraphMaterialization.props(instance.graph()).withDeploy(new Deploy(new RemoteScope(address))),
              range.toString()
      );
      rangeGraphs.put(range, rangeGraph);
    });

    final GraphRouter router = new CoarseRouter(instance.graph(), rangeGraphs);
    acker.tell(router, self());
    rangeGraphs.values().forEach(g -> g.tell(router, self()));
  }

  public static class CoarseRouter implements GraphRouter {
    private final ComposedGraph<AtomicGraph> graph;
    private final IntRangeMap<ActorRef> hashRanges;

    public CoarseRouter(ComposedGraph<AtomicGraph> graph,
                        Map<IntRange, ActorRef> hashRanges) {
      this.graph = graph;
      this.hashRanges = new ListIntRangeMap<>(hashRanges);
    }

    @Override
    public void tell(DataItem<?> message, OutPort source, ActorRef sender) {
      final InPort destination = graph.downstreams().get(source);

      //noinspection rawtypes
      final ToIntFunction hashFunction = destination.hashFunction();

      //noinspection unchecked
      final int hash = hashFunction.applyAsInt(message.payload());
      final AddressedItem result = new AddressedItem(message, destination);

      final ActorRef ref = hashRanges.get(hash);
      ref.tell(result, sender);
    }
  }
}
