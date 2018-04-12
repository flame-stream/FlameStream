package org.apache.zookeeper.server;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public final class ZooKeeperApplication extends ZooKeeperServerMain {
  private final long dirId = ThreadLocalRandom.current().nextLong();
  private final ServerConfig config;

  private ServerCnxnFactory cnxnFactory;

  public ZooKeeperApplication(int port) {
    try {
      final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
      final Properties props = new Properties();
      props.setProperty("clientPort", String.valueOf(port));
      props.setProperty("tickTime", "2000");
      props.setProperty("dataDir", String.valueOf(dirId));
      quorumConfig.parseProperties(props);
      config = new ServerConfig();
      config.readFrom(quorumConfig);
    } catch (IOException | QuorumPeerConfig.ConfigException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() {
    try {
      FileTxnSnapLog txnLog = null;
      final ZooKeeperServer zkServer = new ZooKeeperServer();
      // Registers shutdown handler which will be used to know the
      // server error or shutdown state changes.
      final CountDownLatch shutdownLatch = new CountDownLatch(1);
      zkServer.registerServerShutdownHandler(
              new ZooKeeperServerShutdownHandler(shutdownLatch));

      txnLog = new FileTxnSnapLog(new File(config.dataLogDir), new File(config.dataDir));
      zkServer.setTxnLogFactory(txnLog);
      zkServer.setTickTime(config.tickTime);
      zkServer.setMinSessionTimeout(config.minSessionTimeout);
      zkServer.setMaxSessionTimeout(config.maxSessionTimeout);
      cnxnFactory = ServerCnxnFactory.createFactory();
      cnxnFactory.configure(
              config.getClientPortAddress(),
              config.getMaxClientCnxns()
      );
      cnxnFactory.startup(zkServer);
      final ZooKeeperServer zooKeeperServer = cnxnFactory.getZooKeeperServer();

      synchronized (zkServer) {
        while (zooKeeperServer.state != ZooKeeperServer.State.RUNNING) {
          zooKeeperServer.wait();
        }
      }

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
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
