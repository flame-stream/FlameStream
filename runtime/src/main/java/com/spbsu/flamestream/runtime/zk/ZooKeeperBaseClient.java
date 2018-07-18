package com.spbsu.flamestream.runtime.zk;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.List;

@SuppressWarnings("WeakerAccess")
public class ZooKeeperBaseClient implements AutoCloseable {
  protected static final int MAX_BUFFER_SIZE = 20000;
  protected static final int BUFFER_SIZE = 1000;
  protected static final List<ACL> DEFAULT_ACL = ZKUtil.parseACLs("world:anyone:crwd");

  protected final ZooKeeper zooKeeper;
  protected final Kryo kryo;

  public ZooKeeperBaseClient(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.getFieldSerializerConfig().setIgnoreSyntheticFields(false);
  }

  protected void createIfNotExists(String path) throws KeeperException, InterruptedException {
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

  @Override
  public void close() throws Exception {
    zooKeeper.close();
  }
}
