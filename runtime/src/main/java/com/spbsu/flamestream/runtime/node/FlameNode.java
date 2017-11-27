package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.ZookeeperFlameClient;
import com.spbsu.flamestream.runtime.node.front.FrontManager;
import com.spbsu.flamestream.runtime.node.graph.GraphManager;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;
import org.apache.zookeeper.ZooKeeper;

import java.util.NavigableMap;
import java.util.TreeMap;

class FlameNode extends LoggingActor {
  private final String id;
  private ZookeeperFlameClient zookeeper;

  private final ActorRef frontManager;
  private final ActorRef graphMaterializer;

  private FlameNode(String id, ZooKeeper zk) {
    this.id = id;
    this.zookeeper = new ZookeeperFlameClient(zk);

    this.frontManager = context().actorOf(FrontManager.props(), "fronts");
    this.graphMaterializer = context().actorOf(GraphManager.props(systems()), "graph");
  }

  static Props props(String id, ZooKeeper zooKeeper) {
    return Props.create(FlameNode.class, id, zooKeeper);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }

  private NavigableMap<IntRange, Address> systems() {
    final NavigableMap<IntRange, DumbInetSocketAddress> dns = zookeeper.dns(d -> self().tell(
            d,
            ActorRef.noSender()
    ));

    final NavigableMap<IntRange, Address> systems = new TreeMap<>();
    dns.forEach((s, address) ->
            systems.put(s, new Address("akka.tcp", "worker", address.host(), address.port())));
    return systems;
  }

}
