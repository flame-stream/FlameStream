package com.spbsu.datastream.core.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.front.FrontActor;
import com.spbsu.datastream.core.tick.TickConcierge;
import com.spbsu.datastream.core.tick.TickInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public final class NodeConcierge extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ZooKeeper zooKeeper;
  private final int id;

  @Nullable
  private Map<Integer, ActorPath> nodeConierges = null;

  private ActorRef front = null;

  private NodeConcierge(int id, ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.id = id;
  }

  public static Props props(int id, ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, id, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    nodeConierges = fetchDNS();
    LOG().info("DNS fetched: {}", nodeConierges);

    final Set<Integer> fronts = fetchFronts();
    LOG().info("Fronts fetched: {}", fronts);

    if (fronts.contains(id)) {
      this.front = context().actorOf(FrontActor.props(nodeConierges, id), "front");
    }

    context().actorOf(TickWatcher.props(zooKeeper, self()), "tickWatcher");
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(TickInfo.class, this::onNewTick).build();
  }

  private void onNewTick(TickInfo tickInfo) {
    final String suffix = String.valueOf(tickInfo.startTs());
    final Map<Integer, ActorPath> rangeConcierges = nodeConierges.entrySet().stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().child(suffix)));

    context().actorOf(TickConcierge.props(tickInfo, id, rangeConcierges), suffix);

    if (front != null) {
      front.tell(tickInfo, self());
    }
  }

  private Map<Integer, ActorPath> fetchDNS() throws IOException, KeeperException, InterruptedException {
    final String path = "/dns";
    final byte[] data = zooKeeper.getData(path, false, new Stat());
    final Map<Integer, InetSocketAddress> dns = NodeConcierge.MAPPER
            .readValue(data, new TypeReference<Map<Integer, InetSocketAddress>>() {});

    return dns.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> pathFor(e.getValue())));
  }

  private ActorPath pathFor(InetSocketAddress socketAddress) {
    // TODO: 5/8/17 Properly resolve ActorRef
    final Address address = Address.apply(
            "akka.tcp",
            "worker",
            socketAddress.getAddress().getHostName(),
            socketAddress.getPort()
    );

    return RootActorPath.apply(address, "/")
            .child("user")
            .child("watcher")
            .child("concierge");
  }

  private Set<Integer> fetchFronts() throws KeeperException, InterruptedException, IOException {
    final String path = "/fronts";
    final byte[] data = zooKeeper.getData(path, false, new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Set<Integer>>() {});
  }
}
