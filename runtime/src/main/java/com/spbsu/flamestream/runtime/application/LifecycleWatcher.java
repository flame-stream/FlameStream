package com.spbsu.flamestream.runtime.application;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.FlameNode;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.Set;

import static org.apache.zookeeper.Watcher.Event;

public class LifecycleWatcher extends LoggingActor {
  public static final int SESSION_TIMEOUT = 5000;
  private final String zkConnectString;
  private final String id;

  private ZooKeeperFlameClient client = null;

  @Nullable
  private ActorRef flameNode = null;

  private LifecycleWatcher(String id, String zkConnectString) {
    this.zkConnectString = zkConnectString;
    this.id = id;
  }

  public static Props props(String id, String zkConnectString) {
    return Props.create(LifecycleWatcher.class, id, zkConnectString);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    client = new ZooKeeperFlameClient(new ZooKeeper(
            zkConnectString,
            SESSION_TIMEOUT,
            event -> self().tell(event, self())
    ));
  }

  @Override
  public void postStop() {
    super.postStop();
    try {
      client.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(WatchedEvent.class, this::onWatchedEvent)
            .match(Graph.class, this::onGraph)
            .match(AttachFront.class, f -> {
              if (flameNode != null) {
                log().info("Attaching front {}", f);
                flameNode.tell(f, self());
              } else {
                throw new IllegalStateException();
              }
            })
            .match(AttachRear.class, r -> {
              if (flameNode != null) {
                log().info("Attaching rear {}", r);
                flameNode.tell(r, self());
              } else {
                throw new IllegalStateException();
              }
            })
            .build();
  }

  private void onGraph(Graph graph) {
    final ClusterConfig config = client.config();
    if (flameNode == null) {
      log().info("Creating node with graph: '{}', config: '{}'", graph, config);
      flameNode = context().actorOf(FlameNode.props(id, graph, config, client), "node");

      final Set<AttachFront<?>> initialFronts = client.fronts(newFronts ->
              newFronts.forEach(front -> self().tell(front, ActorRef.noSender()))
      );
      initialFronts.forEach(f -> self().tell(f, ActorRef.noSender()));

      final Set<AttachRear<?>> initialRears = client.rears(newRears ->
              newRears.forEach(rear -> self().tell(rear, ActorRef.noSender()))
      );
      initialRears.forEach(r -> self().tell(r, ActorRef.noSender()));
    } else {
      throw new IllegalStateException("Multiple graphs deployment detected");
    }
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();

      switch (state) {
        case SyncConnected:
          log().info("Connected to ZK");
          final Optional<Graph> graph = client.graph(g -> self().tell(g, ActorRef.noSender()));
          graph.ifPresent(g -> self().tell(g, ActorRef.noSender()));
          break;
        case Expired:
          log().info("Session expired");
          context().stop(self());
          break;
        default:
          unhandled(event);
      }
    } else {
      unhandled(event);
    }
  }
}
