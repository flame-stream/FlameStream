package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.node.front.FrontManager;
import com.spbsu.flamestream.runtime.node.materializer.GraphMaterializer;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.zookeeper.ZooKeeper;

class FlameNode extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final ZooKeeper zk;
  private final String id;

  private final ActorRef zookeeperWatcher;
  private final ActorRef frontManager;
  private final ActorRef graphMaterializer;

  private FlameNode(String id, ZooKeeper zk) {
    this.zk = zk;
    this.id = id;
    this.zookeeperWatcher = context().actorOf(ZookeeperWatcher.props(zk, self()), "zkWatcher");
    this.frontManager = context().actorOf(FrontManager.props(), "frontManager");
    this.graphMaterializer = context().actorOf(GraphMaterializer.props());
  }

  static Props props(String id, ZooKeeper zooKeeper) {
    return Props.create(FlameNode.class, id, zooKeeper);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }
}
