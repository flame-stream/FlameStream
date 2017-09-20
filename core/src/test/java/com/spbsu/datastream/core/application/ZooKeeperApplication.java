package com.spbsu.datastream.core.application;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.util.Properties;

public final class ZooKeeperApplication extends ZooKeeperServerMain {
  public void run() throws IOException {
    final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();

    try {
      final Properties props = new Properties();
      props.setProperty("clientPort", "2181");
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
  }
}
