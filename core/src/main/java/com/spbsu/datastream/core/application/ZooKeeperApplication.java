package com.spbsu.datastream.core.application;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ZooKeeperApplication extends ZooKeeperServerMain {
  public void run() throws IOException {
    final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();

    try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream("zookeeper-dev.properties")) {
      final Properties props = new Properties();
      props.load(stream);
      quorumConfig.parseProperties(props);
    } catch (QuorumPeerConfig.ConfigException | IOException e) {
      throw new RuntimeException(e);
    }

    final ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(quorumConfig);

    this.runFromConfig(serverConfig);
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
