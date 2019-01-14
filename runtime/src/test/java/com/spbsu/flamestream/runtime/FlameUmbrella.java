package com.spbsu.flamestream.runtime;

import akka.actor.ActorKilledException;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Kill;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.config.AckerConfig;
import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.master.acker.Registry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class Cluster extends LoggingActor {
  private final ActorRef inner;
  private final boolean blinking;

  private Cluster(Graph g,
                  StateStorage stateStorage,
                  int parallelism,
                  int maxElementsInGraph,
                  int millisBetweenCommits,
                  boolean blinking) {
    this.blinking = blinking;

    System.out.println("cluster parallelism: " + parallelism);
    System.out.println("cluster maxElementsInGraph: " + maxElementsInGraph);

    final Map<String, HashGroup> ranges = new HashMap<>();
    final Map<String, ActorPath> paths = new HashMap<>();
    final List<HashUnit> ra = HashUnit.covering(parallelism).collect(Collectors.toList());
    System.out.println("cluster ra: " + ra);
    for (int i = 0; i < parallelism; ++i) {
      final String id = "node-" + i;
      final HashUnit range = ra.get(i);
      paths.put(
              id,
              RootActorPath.apply(context().system().provider().getDefaultAddress(), "/")
                      .child("user")
                      .child("restarter")
                      .child("cluster")
                      .child(id)
      );
      ranges.put(id, new HashGroup(Collections.singleton(range)));
    }
    System.out.println("cluster ranges: " + ranges);
    System.out.println("cluster paths: " + paths);
    final ClusterConfig clusterConfig = new ClusterConfig(paths, "node-0", ranges);
    final AckerConfig ackerConfig = new AckerConfig(maxElementsInGraph, millisBetweenCommits, 0);

    final Registry registry = new InMemoryRegistry();
    final Map<String, Props> nodeProps = new HashMap<>();
    paths.keySet().forEach(id -> {
      final Props props = FlameNode.props(id, g, clusterConfig, ackerConfig, registry, stateStorage);
      nodeProps.put(id, props);
    });
    System.out.println("cluster node props: " + nodeProps);
    inner = context().actorOf(FlameUmbrella.props(nodeProps, paths), "cluster");
  }

  static Props props(Graph g,
                     StateStorage stateStorage,
                     int parallelism,
                     int maxElementsInGraph,
                     int millisBetweenCommits,
                     boolean blinking) {
    return Props.create(
            Cluster.class,
            g,
            stateStorage,
            parallelism,
            maxElementsInGraph,
            millisBetweenCommits,
            blinking
    );
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .matchAny(m -> {
              System.out.format("cluster got %s%n", m);
              inner.forward(m, context());
            })
            .build();
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    if (blinking) {
      context().system()
              .scheduler()
              .schedule(
                      FiniteDuration.apply(10, TimeUnit.SECONDS),
                      FiniteDuration.apply(10, TimeUnit.SECONDS),
                      inner,
                      Kill.getInstance(),
                      context().system().dispatcher(),
                      self()
              );
    }
  }

  private static final SupervisorStrategy strategy =
          new OneForOneStrategy(100000, Duration.create(1, TimeUnit.MINUTES), DeciderBuilder
                  .match(ActorKilledException.class, e -> SupervisorStrategy.restart())
                  .matchAny(o -> SupervisorStrategy.escalate()).build());

  @Override
  public SupervisorStrategy supervisorStrategy() {
    return strategy;
  }
}

class FlameUmbrella extends LoggingActor {
  private final List<Object> toBeTold;
  private final Map<String, ActorPath> paths;

  private FlameUmbrella(Map<String, Props> props, Map<String, ActorPath> paths, List<Object> toBeTold) {
    System.out.format("FlameUmbrella props: %s%n", props);
    System.out.format("FlameUmbrella paths: %s%n", paths);
    System.out.format("FlameUmbrella toBeTold: %s%n", toBeTold);

    this.paths = paths;
    this.toBeTold = toBeTold;
    props.forEach((id, prop) -> {
      System.out.format("FlameUmbrella create actor %s, props %s%n", id, prop);
      context().actorOf(prop, id);
    });

    // Reattach rears first
    toBeTold.stream().filter(e -> e instanceof AttachRear).forEach(a -> {
      getContext().getChildren().forEach(c -> c.tell(a, self()));
    });
    toBeTold.stream().filter(e -> e instanceof AttachFront).forEach(a -> {
      getContext().getChildren().forEach(c -> c.tell(a, self()));
    });
  }

  public static Props props(Map<String, Props> props, Map<String, ActorPath> paths) {
    return Props.create(FlameUmbrella.class, props, paths, new ArrayList<>());
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontTypeWithId.class, a -> {
              System.out.format("FlameUmbrella got FrontTypeWithId %s from %s%n", a, context().sender());
              System.out.format("FlameUmbrella front toBeTold %s%n", toBeTold);
              final AttachFront attach = new AttachFront<>(a.id, a.type.instance());

              toBeTold.add(attach);
              getContext().getChildren().forEach(n -> {
                System.out.format("FlameUmbrella front child: %s%n", n);
                n.tell(attach, self());
              });
              final List<Object> collect = paths.entrySet().stream()
                      .map(node -> a.type.handle(new SystemEdgeContext(node.getValue(), node.getKey(), a.id)))
                      .collect(Collectors.toList());
              System.out.format("FlameUmbrella front collect %s%n", collect);
              collect.forEach(System.out::println);
              sender().tell(collect, self());
            })
            .match(RearTypeWithId.class, a -> {
              System.out.format("FlameUmbrella got RearTypeWithId %s%n", a);
              System.out.format("FlameUmbrella rear toBeTold: %s%n", toBeTold);
              final AttachRear attach = new AttachRear(a.id, a.type.instance());
              toBeTold.add(attach);
              getContext().getChildren().forEach(n -> {
                System.out.format("FlameUmbrella rear child: %s%n", n);
                n.tell(attach, self());
              });
              final List<Object> collect = paths.entrySet().stream()
                      .map(node -> a.type.handle(new SystemEdgeContext(node.getValue(), node.getKey(), a.id)))
                      .collect(Collectors.toList());
      //        System.out.format("FlameUmbrella rear collect %s%n", collect);
              collect.forEach(System.out::println);
              sender().tell(collect, self());
            })
            .build();
  }

  static class FrontTypeWithId<F extends Front, H> {
    public final String id;
    public final FlameRuntime.FrontType<F, H> type;

    public FrontTypeWithId(String id, FlameRuntime.FrontType<F, H> type) {
      this.id = id;
      this.type = type;
    }

    @Override
    public String toString() {
      return String.format("FrontTypeWithId id %s type %s%n", id, type);
    }
  }

  static class RearTypeWithId<R extends Rear, H> {
    public final String id;
    public final FlameRuntime.RearType<R, H> type;

    RearTypeWithId(String id, FlameRuntime.RearType<R, H> type) {
      this.id = id;
      this.type = type;
    }

    @Override
    public String toString() {
      return String.format("RearTypeWithId. id: %s, type: %s", id, type);
    }
  }
}


class InMemoryRegistry implements Registry {
  private final Map<EdgeId, Long> linearizableCollection = new HashMap<>();
  private long lastCommit = 0;

  @Override
  public String toString() {
    return String.format("InMemoryRegistry lastCommit %d, linearizableCollection %s", lastCommit, linearizableCollection);
  }

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    linearizableCollection.compute(frontId, (edgeId, time) -> {
      if (time != null) {
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
