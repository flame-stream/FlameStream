package org.apache.zookeeper.server;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public final class ZooKeeperApplication extends ZooKeeperServerMain {
  private final int port;

  public ZooKeeperApplication(int port) {
    this.port = port;
  }

  public void run() throws IOException {
    final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();

    try {
      try {
        FileUtils.deleteDirectory(new File("./zookeeper"));
      } catch (IOException e) {

      }
      final Properties props = new Properties();
      props.setProperty("clientPort", String.valueOf(port));
      props.setProperty("tickTime", "2000");
      props.setProperty("dataDir", "./zookeeper");
      quorumConfig.parseProperties(props);
    } catch (QuorumPeerConfig.ConfigException | IOException e) {
      throw new RuntimeException(e);
    }

    final ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(quorumConfig);

    runFromConfig(serverConfig);
  }

  @Override
  public void shutdown() {
    super.shutdown();
    try {
      FileUtils.deleteDirectory(new File("./zookeeper"));
    } catch (IOException e) {

    }
  }
}
