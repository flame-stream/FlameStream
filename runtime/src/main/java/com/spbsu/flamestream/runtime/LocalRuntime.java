package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.acker.Registry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.state.InMemStateStorage;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.SyncKiller;
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
  private final int millisBetweenCommits;
  private final ActorSystem system;
  private final Registry registry = new InMemoryRegistry();
  private final StateStorage stateStorage;
  private LocalRuntime(int parallelism,
                       int maxElementsInGraph,
                       int millisBetweenCommits,
                       ActorSystem system,
                       StateStorage stateStorage) {
    this.parallelism = parallelism;
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.system = system;
    this.stateStorage = stateStorage;
  }

  public ActorSystem system() {
    return system;
  }

  @Override
  public Flame run(Graph g) {
    final List<HashUnit> ra = HashUnit.covering(parallelism).collect(Collectors.toList());
    final Map<String, ActorPath> paths = new HashMap<>();
    final Map<String, HashGroup> ranges = new HashMap<>();
    for (int i = 0; i < parallelism; ++i) {
      final String id = "node-" + i;
      final HashUnit range = ra.get(i);
      paths.put(
              id,
              RootActorPath.apply(Address.apply("akka", system.name()), "/")
                      .child("user")
                      .child(id)
      );
      ranges.put(id, new HashGroup(Collections.singleton(range)));
    }

    final ClusterConfig clusterConfig = new ClusterConfig(
            paths,
            "node-0",
            new ComputationProps(ranges, maxElementsInGraph),
            millisBetweenCommits,
            0
    );

    final List<ActorRef> nodes = paths.keySet().stream()
            // FIXME: 3/1/18 real storage
            .map(id -> system.actorOf(FlameNode.props(id, g, clusterConfig, registry, stateStorage)
                    .withDispatcher("util-dispatcher"), id))
            .collect(Collectors.toList());

    return new Flame() {
      @Override
      public void close() {
        nodes.forEach(n -> SyncKiller.syncKill(n, system));
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

  private static class InMemoryRegistry implements Registry {
    private final Map<EdgeId, Long> linearizableCollection = new HashMap<>();
    private long lastCommit = 0;

    @Override
    public void register(EdgeId frontId, long attachTimestamp) {
      linearizableCollection.compute(frontId, (edgeId, aLong) -> {
        if (aLong != null) {
          throw new IllegalArgumentException("Front " + frontId + " has been already registered");
        }
        return attachTimestamp;
      });
    }

    @Override
    public long registeredTime(EdgeId frontId) {
      return linearizableCollection.getOrDefault(frontId, -1L);
    }

    @Override
    public void committed(long time) {
      if (time < lastCommit) {
        throw new IllegalArgumentException("Not monotonic last commit time, expected " + time + " < " + lastCommit);
      }
      lastCommit = time;
    }

    @Override
    public long lastCommit() {
      return lastCommit;
    }
  }

  public static class Builder {
    private int parallelism = DEFAULT_PARALLELISM;
    private int maxElementsInGraph = DEFAULT_MAX_ELEMENTS_IN_GRAPH;
    private int millisBetweenCommits = DEFAULT_MILLIS_BETWEEN_COMMITS;
    private StateStorage stateStorage = null;
    private ActorSystem system = null;

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder maxElementsInGraph(int maxElementsInGraph) {
      this.maxElementsInGraph = maxElementsInGraph;
      return this;
    }

    public Builder millisBetweenCommits(int millisBetweenCommits) {
      this.millisBetweenCommits = millisBetweenCommits;
      return this;
    }

    public Builder system(ActorSystem system) {
      this.system = system;
      return this;
    }

    public Builder stateStorage(StateStorage stateStorage) {
      this.stateStorage = stateStorage;
      return this;
    }

    public LocalRuntime build() {
      if (stateStorage == null) {
        stateStorage = new InMemStateStorage();
      }
      if (system == null) {
        system = ActorSystem.create("local-runtime", ConfigFactory.load("local"));
      }
      return new LocalRuntime(parallelism, maxElementsInGraph, millisBetweenCommits, system, stateStorage);
    }
  }
}
