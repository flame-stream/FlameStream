package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.front.FrontActor;
import com.spbsu.flamestream.runtime.front.FrontConcierge;
import com.spbsu.flamestream.runtime.tick.TickCommitDone;
import com.spbsu.flamestream.runtime.tick.TickConcierge;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public final class NodeConcierge extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ZooKeeper zk;
  private final int id;

  private final Set<Long> committedTicks = new HashSet<>();

  @Nullable
  private Map<Integer, ActorPath> nodeConierges = null;

  @Nullable
  private ActorRef tickWatcher = null;

  private NodeConcierge(int id, ZooKeeper zk) {
    this.zk = zk;
    this.id = id;
  }

  public static Props props(int id, ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, id, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    nodeConierges = fetchDns();
    log().info("DNS fetched: {}", nodeConierges);

    tickWatcher = context().actorOf(TickWatcher.props(zk, self()), "tickWatcher");
    context().actorOf(FrontConcierge.props(id, zk), "fronts");
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(TickInfo.class, this::onNewTick)
            .match(TickCommitDone.class, this::onTickCommitted)
            .build();
  }

  private void onNewTick(TickInfo tickInfo) {
    final String suffix = String.valueOf(tickInfo.id());
    final Map<Integer, ActorPath> rangeConcierges = nodeConierges.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().child(suffix)));

    final ActorRef tickConcierge = context().actorOf(
            TickConcierge.props(tickInfo, id, rangeConcierges, tickWatcher),
            suffix
    );

    //FIXME: in long term future we need to store every single tick.
    committedTicks.forEach(t -> tickConcierge.tell(new TickCommitDone(t), self()));
  }

  private void onTickCommitted(TickCommitDone committed) {
    committedTicks.add(committed.tickId());
    getContext().getChildren().forEach(c -> c.tell(committed, sender()));
  }

  private Map<Integer, ActorPath> fetchDns() throws IOException, KeeperException, InterruptedException {
    final String path = "/dns";
    final byte[] data = zk.getData(path, false, new Stat());
    final Map<Integer, DumbInetSocketAddress> dns = NodeConcierge.MAPPER.readValue(
            data,
            new TypeReference<Map<Integer, DumbInetSocketAddress>>() {}
    );

    return dns.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> pathFor(e.getValue())));
  }

  private ActorPath pathFor(DumbInetSocketAddress socketAddress) {
    final Address address = Address.apply("akka.tcp", "worker", socketAddress.host(), socketAddress.port());
    return RootActorPath.apply(address, "/").child("user").child("watcher").child("concierge");
  }
}
