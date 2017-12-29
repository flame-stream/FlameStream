package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.PoisonPill;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalRuntime implements FlameRuntime {
  private final int parallelism;
  private final int maxElementsInGraph;
  private final ActorSystem system;

  public LocalRuntime(int parallelism) {
    this(ActorSystem.create("local-runtime",
            ConfigFactory.load("local")),
            parallelism,
            DEFAULT_MAX_ELEMENTS_IN_GRAPH);
  }

  public LocalRuntime(int parallelism, int maxElementsInGraph) {
    this(
            ActorSystem.create("local-runtime", ConfigFactory.load("local")),
            parallelism,
            maxElementsInGraph
    );
  }

  private LocalRuntime(ActorSystem system, int parallelism, int maxElementsInGraph) {
    this.system = system;
    this.parallelism = parallelism;
    this.maxElementsInGraph = maxElementsInGraph;
  }

  public ActorSystem system() {
    return system;
  }

  @Override
  public Flame run(Graph g) {
    final List<HashRange> ra = HashRange.covering(parallelism).collect(Collectors.toList());
    final Map<String, ActorPath> paths = new HashMap<>();
    final Map<String, HashRange> ranges = new HashMap<>();
    for (int i = 0; i < parallelism; ++i) {
      final String id = "node-" + i;
      final HashRange range = ra.get(i);
      paths.put(
              id,
              RootActorPath.apply(Address.apply("akka", system.name()), "/")
                      .child("user")
                      .child(id)
      );
      ranges.put(id, range);
    }

    final ClusterConfig clusterConfig = new ClusterConfig(
            paths,
            "node-0",
            new ComputationProps(ranges, maxElementsInGraph)
    );
    final AttachRegistry registry = new InMemoryRegistry();

    final List<ActorRef> nodes = paths.keySet().stream()
            .map(id -> system.actorOf(FlameNode.props(id, g, clusterConfig, registry)
                    .withDispatcher("resolver-dispatcher"), id))
            .collect(Collectors.toList());

    return new Flame() {
      @Override
      public void close() {
        nodes.forEach(n -> n.tell(PoisonPill.getInstance(), ActorRef.noSender()));
      }

      @Override
      public <F extends Front, H> Stream<H> attachFront(String id, FrontType<F, H> type) {
        nodes.forEach(n -> n.tell(new AttachFront<>(id, type.instance()), ActorRef.noSender()));
        return paths.entrySet().stream()
                .map(node -> type.handle(new SystemEdgeContext(node.getValue(), node.getKey(), id)));
      }

      @Override
      public <R extends Rear, H> Stream<H> attachRear(String id, RearType<R, H> type) {
        nodes.forEach(n -> n.tell(new AttachRear<>(id, type.instance()), ActorRef.noSender()));
        return paths.entrySet().stream()
                .map(node -> type.handle(new SystemEdgeContext(node.getValue(), node.getKey(), id)));
      }
    };
  }

  @Override
  public void close() {
    try {
      Await.ready(system.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private static class InMemoryRegistry implements AttachRegistry {
    private final Map<EdgeId, Long> linearizableCollection = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void register(EdgeId frontId, long attachTimestamp) {
      linearizableCollection.put(frontId, attachTimestamp);
    }
  }
}
