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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.zookeeper.Watcher.Event;

public class LifecycleWatcher extends LoggingActor {
  private static final int SESSION_TIMEOUT = 5000;
  private final String zkConnectString;
  private final String id;

  private ZooKeeperGraphClient client = null;

  private final Map<String, ActorRef> nodes = new HashMap<>();

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
    client = new ZooKeeperGraphClient(new ZooKeeper(
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
            .match(List.class, graphs -> {
              final List<ZooKeeperGraphClient.ZooKeeperFlameClient> clients =
                      (List<ZooKeeperGraphClient.ZooKeeperFlameClient>) graphs;

              final Set<String> activeNames = clients.stream().map(s -> s.name()).collect(Collectors.toSet());
              final Set<String> toBeKilled = nodes.keySet()
                      .stream()
                      .filter(n -> !activeNames.contains(n))
                      .collect(Collectors.toSet());
              toBeKilled.forEach(name -> {
                context().stop(nodes.get(name));
                nodes.remove(name);
              });

              clients.stream().filter(n -> !nodes.keySet().contains(n.name())).forEach(this::initGraph);
            })
            .match(WatchedEvent.class, this::onWatchedEvent)
            .build();
  }

  private void initGraph(ZooKeeperGraphClient.ZooKeeperFlameClient flameClient) {
    final ClusterConfig config = client.config().withChildPath(flameClient.name());
    final Graph g = flameClient.graph();
    log().info("Creating node with watchGraphs: '{}', config: '{}'", g, config);
    final ActorRef node = context().actorOf(FlameNode.props(id, g, config, flameClient).withDispatcher("resolver-dispatcher"), flameClient.name());
    nodes.put(flameClient.name(), node);

    final Set<AttachFront<?>> initialFronts = flameClient.fronts(newFronts ->
            newFronts.forEach(front -> node.tell(front, self()))
    );
    initialFronts.forEach(f -> node.tell(f, self()));

    final Set<AttachRear<?>> initialRears = flameClient.rears(newRears ->
            newRears.forEach(rear -> node.tell(rear, self()))
    );
    initialRears.forEach(r -> node.tell(r, self()));
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();

      switch (state) {
        case SyncConnected:
          log().info("Connected to ZK");
          final List<ZooKeeperGraphClient.ZooKeeperFlameClient> graphs = client.watchGraphs(g -> self().tell(
                  g,
                  ActorRef.noSender()
          ));
          if (!graphs.isEmpty()) {
            self().tell(graphs, ActorRef.noSender());
          }
          break;
        case Expired:
          log().info("Session expired");
          context().stop(self());
          break;
        case Disconnected:
          log().info("Disconnected");
          break;
        default:
          unhandled(event);
      }
    } else {
      unhandled(event);
    }
  }
}
