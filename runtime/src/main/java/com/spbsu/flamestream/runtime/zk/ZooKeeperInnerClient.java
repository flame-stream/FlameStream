package com.spbsu.flamestream.runtime.zk;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.FlameRuntime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperInnerClient extends ZooKeeperBaseClient {
  public ZooKeeperInnerClient(ZooKeeper zooKeeper) {
    super(zooKeeper);
  }

  public void push(Graph graph) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, graph);
      zooKeeper.create(
              "/graph",
              o.toBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public Graph graph() {
    try {
      final byte[] data = zooKeeper.getData(
              "/graph",
              false,
              null
      );
      final ByteBufferInput input = new ByteBufferInput(data);
      return (Graph) kryo.readClassAndObject(input);
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void attachFront(String name, FlameRuntime.FrontInstance<?> instance) {
    try {
      createIfNotExists("/graph/fronts");
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, instance);

      zooKeeper.create(
              "/graph/fronts/" + name,
              o.toBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void attachRear(String name, FlameRuntime.RearInstance<?> instance) {
    try {
      createIfNotExists("/graph/rears");
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, instance);

      zooKeeper.create(
              "/graph/rears/" + name,
              o.toBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }
}