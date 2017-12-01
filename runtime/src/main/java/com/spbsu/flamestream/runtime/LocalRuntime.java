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
import com.spbsu.flamestream.runtime.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalRuntime implements FlameRuntime {
  private final int parallelism;
  private final ActorSystem system;

  public LocalRuntime(int parallelism) {
    this(ActorSystem.create(), parallelism);
  }

  private LocalRuntime(ActorSystem system, int parallelism) {
    this.parallelism = parallelism;
    this.system = system;
  }

  public int parallelism() {
    return parallelism;
  }

  public ActorSystem system() {
    return system;
  }

  @Override
  public Flame run(Graph g) {
    final Set<ActorRef> nodes = nodes(g);
    return new Flame() {
      @Override
      public void extinguish() {
        nodes.forEach(n -> n.tell(PoisonPill.getInstance(), ActorRef.noSender()));
      }

      @Override
      public <T extends Front, H extends FrontHandle> Stream<H> attachFront(String id,
                                                                            Class<T> front,
                                                                            String... args) {
        nodes.forEach(n -> n.tell(new FrontInstance<>(id, front, args), ActorRef.noSender()));
        return Stream.empty();
      }

      @Override
      public <T extends Rear, H extends RearHandle> Stream<H> attachRear(String id,
                                                                         Class<T> rear,
                                                                         String... args) {
        nodes.forEach(n -> n.tell(new RearInstance<>(id, rear, args), ActorRef.noSender()));
        return Stream.empty();
      }
    };
  }

  private Set<ActorRef> nodes(Graph graph) {
    final List<HashRange> ranges = HashRange.covering(parallelism).collect(Collectors.toList());
    final Map<String, ActorPath> paths = new HashMap<>();
    final Map<String, HashRange> rangeMap = new HashMap<>();
    for (int i = 0; i < parallelism; ++i) {
      final String id = "node-" + i;
      final HashRange range = ranges.get(i);
      paths.put(
              id,
              RootActorPath.apply(Address.apply("akka", system.name()), "/")
                      .child("user")
                      .child(id)
      );
      rangeMap.put(id, range);
    }

    final ClusterConfig clusterConfig = new ClusterConfig(paths, "node-0", new ComputationLayout(rangeMap));
    final AttachRegistry registry = new InMemoryRegistry();
    final Set<ActorRef> nodes = new HashSet<>();
    paths.keySet().forEach(id -> nodes.add(
            system.actorOf(FlameNode.props(id, graph, clusterConfig, registry), id))
    );
    return nodes;
  }

  private static class InMemoryRegistry implements AttachRegistry {
    private final Map<String, Long> linearizableCollection = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void register(String frontId, long attachTimestamp) {
      linearizableCollection.put(frontId, attachTimestamp);
    }
  }
}
