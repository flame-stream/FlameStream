package com.spbu.flamestream.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.jooq.lambda.Unchecked;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class ZkFlameClient implements FlameClient {
  private static final int MAX_BUFFER_SIZE = 20000;
  private static final int BUFFER_SIZE = 1000;
  private static final List<ACL> DEFAULT_ACL = ZKUtil.parseACLs("world:anyone:crwd");

  private final ZooKeeper zooKeeper;
  private final Kryo kryo;

  @SuppressWarnings("WeakerAccess")
  public ZkFlameClient(String zkString, int sessionTimeout) {
    try {
      this.zooKeeper = new ZooKeeper(zkString, sessionTimeout, event -> {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void push(Job job) {
    try {
      { //push graph
        final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
        kryo.writeClassAndObject(o, job.graph());
        zooKeeper.create(
                "/client_graph",
                o.toBytes(),
                DEFAULT_ACL,
                CreateMode.PERSISTENT
        );
      }
      { //push fronts
        createIfNotExists("/client_graph/client_fronts");
        job.fronts().forEach(Unchecked.consumer(front -> {
          final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
          kryo.writeObject(o, new InetSocketAddress(front.host(), front.port()));
          zooKeeper.create(
                  "/client_graph/client_fronts/" + front.id(),
                  o.toBytes(),
                  DEFAULT_ACL,
                  CreateMode.PERSISTENT
          );
        }));
      }
      { //push rears
        createIfNotExists("/client_graph/client_rears");
        job.rears().forEach(Unchecked.consumer(rear -> {
          final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
          kryo.writeClassAndObject(o, new InetSocketAddress(rear.host(), rear.port()));
          zooKeeper.create(
                  "/client_graph/client_rears/" + rear.id(),
                  o.toBytes(),
                  DEFAULT_ACL,
                  CreateMode.PERSISTENT
          );
        }));
      }
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  private void createIfNotExists(String path) throws KeeperException, InterruptedException {
    try {
      zooKeeper.create(
              path,
              new byte[0],
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (KeeperException k) {
      if (k.code() != KeeperException.Code.NODEEXISTS) {
        throw k;
      }
    }
  }
}
