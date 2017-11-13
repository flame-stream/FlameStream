package com.spbsu.flamestream.runtime.front;

import akka.actor.Props;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import org.apache.zookeeper.ZooKeeper;

public final class FrontConcierge extends LoggingActor {
  private final ZooKeeper zk;
  private final int nodeId;

  private FrontConcierge(int nodeId, ZooKeeper zk) {
    this.nodeId = nodeId;
    this.zk = zk;
  }

  public static Props props(int nodeId, ZooKeeper zk) {
    return Props.create(nodeId, zk);
  }

  @Override
  public Receive createReceive() {
    return null;
  }
}
