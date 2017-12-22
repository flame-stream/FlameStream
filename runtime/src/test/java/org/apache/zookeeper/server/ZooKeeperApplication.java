package org.apache.zookeeper.server;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public final class ZooKeeperApplication extends ZooKeeperServerMain {
  private final int port;
  private final long dirId = ThreadLocalRandom.current().nextLong();

  public ZooKeeperApplication(int port) {
    this.port = port;
  }

  public void run() {
    try {
      final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
      final Properties props = new Properties();
      props.setProperty("clientPort", String.valueOf(port));
      props.setProperty("tickTime", "2000");
      props.setProperty("dataDir", String.valueOf(dirId));
      quorumConfig.parseProperties(props);

      final ServerConfig serverConfig = new ServerConfig();
      serverConfig.readFrom(quorumConfig);

      runFromConfig(serverConfig);
    } catch (QuorumPeerConfig.ConfigException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    try {
      FileUtils.deleteDirectory(new File(String.valueOf(dirId)));
    } catch (Exception e) {
    }
  }
}
