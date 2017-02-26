package experiments.nikita.zookeeper;

import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ClientTest {
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    new ClientTest().run();
  }

  private void run() throws IOException, KeeperException, InterruptedException {
    final ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, new LogWatcher());
    final List<ACL> acl = ZKUtil.parseACLs("world:anyone:crdwa");
    zk.create("/testNode", "hi".getBytes(), acl, CreateMode.PERSISTENT);
  }

  private static class LogWatcher implements Watcher {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    @Override
    public void process(final WatchedEvent watchedEvent) {
      LOG.info(watchedEvent.toString());
    }
  }
}
