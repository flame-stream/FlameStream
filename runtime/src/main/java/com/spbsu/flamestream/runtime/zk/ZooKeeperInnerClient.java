package com.spbsu.flamestream.runtime.zk;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.master.acker.Registry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperInnerClient extends ZooKeeperBaseClient implements Registry {
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

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    try {
      zooKeeper.create(
              "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId(),
              new byte[0],
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );

      zooKeeper.create(
              "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs",
              Long.toString(attachTimestamp).getBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long registeredTime(EdgeId frontId) {
    try {
      final String frontPath = "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs";
      final Stat exists = zooKeeper.exists(frontPath, false);
      if (exists != null) {
        return Long.parseLong(new String(zooKeeper.getData(frontPath, false, null)));
      } else {
        return -1;
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void committed(long time) {
    try {
      final String lastCommitPath = "/graph/last-commit";
      createIfNotExists(lastCommitPath);
      zooKeeper.setData(lastCommitPath, Long.toString(time).getBytes(), -1);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long lastCommit() {
    try {
      final String lastCommitPath = "/graph/last-commit";
      final Stat exists = zooKeeper.exists(lastCommitPath, false);
      if (exists != null) {
        return Long.parseLong(new String(zooKeeper.getData(lastCommitPath, false, null)));
      } else {
        return 0;
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}