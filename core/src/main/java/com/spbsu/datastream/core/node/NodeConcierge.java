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
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImpl;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class NodeConcierge extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ZooKeeper zooKeeper;
  private final int id;

  @Nullable
  private Map<Integer, ActorPath> nodeConcierges = null;

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
    nodeConcierges = fetchDNS();
    LOG().info("DNS fetched: {}", nodeConcierges);

    final Set<Integer> fronts = fetchFronts();
    LOG().info("Fronts fetched: {}", fronts);

    if (fronts.contains(id)) {
      this.front = context().actorOf(FrontActor.props(nodeConcierges, id), "front");
    }

    context().actorOf(TickWatcher.props(zooKeeper, self()), "tickWatcher");
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(TickInfo.class, this::onNewTick).build();
  }

  private void onNewTick(TickInfo tickInfo) {
    context().actorOf(TickConcierge.props(tickInfo, id, nodeConcierges), String.valueOf(tickInfo.startTs()));

    if (front != null) {
      front.tell(tickInfo, self());
    }
  }

  private Map<Integer, ActorPath> fetchDNS() throws IOException, KeeperException, InterruptedException {
    final String path = "/dns";
    final byte[] data = zooKeeper.getData(path, false, new Stat());
    final Map<Integer, InetSocketAddress> dns = NodeConcierge.MAPPER
            .readValue(data, new TypeReference<Map<Integer, InetSocketAddress>>() {});

    return dns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> pathFor(e.getValue())));
  }

  private ActorPath pathFor(InetSocketAddress socketAddress) {
    // TODO: 5/8/17 Properly resolve ActorRef
    final Address address = Address.apply("akka.tcp", "worker", socketAddress.getAddress().getHostName(), address.getPort());
    return RootActorPath.apply(address, "/")
            .child("user")
            .child("watcher")
            .child("concierge")
            .child("dns");
  }

  private Set<Integer> fetchFronts() throws KeeperException, InterruptedException, IOException {
    final String path = "/fronts";
    final byte[] data = zooKeeper.getData(path, false, new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Set<Integer>>() {});
  }
}
