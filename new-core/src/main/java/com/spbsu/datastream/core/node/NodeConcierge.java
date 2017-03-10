package com.spbsu.datastream.core.node;

import akka.actor.Props;
import akka.actor.UntypedActor;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.net.InetSocketAddress;
import java.util.List;

public class NodeConcierge extends UntypedActor {
  private final ZooKeeper zooKeeper;
  private final InetSocketAddress address;

  private NodeConcierge(final InetSocketAddress address, final ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.address = address;
  }

  public static Props props(final InetSocketAddress address, final ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, address, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    final String path = "/member/" + address.getHostString() + ":" + address.getPort();
    final String data = "worker";
    final List<ACL> acl = ZKUtil.parseACLs("world:anyone:r");
    zooKeeper.create(path, data.getBytes(), acl, CreateMode.EPHEMERAL);
  }

  public void connecting(final Object message) {

  }

  public void connected(final Object message) {

  }

  @Override
  public void onReceive(final Object message) throws Throwable {
  }
}
