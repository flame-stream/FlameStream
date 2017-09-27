package com.spbsu.flamestream.config;

import com.typesafe.config.Config;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class TypesafeEnvironmentCfg implements EnvironmentCfg {
  private final boolean local;
  private final LocalEnvironmentCfg localEnvironmentCfg;
  private final RemoteEnvironmentCfg remoteEnvironmentCfg;

  public TypesafeEnvironmentCfg(Config load) {
    final Config config = load.getConfig("cluster");
    this.local = config.getBoolean("local");
    this.localEnvironmentCfg = new LocalEnvironmentCfg(config.getInt("local-cluster.workers"), config.getInt("local-cluster.workers"));
    this.remoteEnvironmentCfg = new RemoteEnvironmentCfg(
            config.getString("real-cluster.zk"),
            config.getConfigList("real-cluster.nodes").stream()
                    .collect(Collectors.toMap(cfg -> cfg.getInt("id"), cfg -> new InetSocketAddress(cfg.getString("host"), cfg.getInt("port")))),
            new HashSet<>(config.getIntList("real-cluster.fronts"))
    );
  }

  @Override
  public boolean isLocal() {
    return local;
  }

  @Override
  public LocalEnvironmentCfg localClusterCfg() {
    return localEnvironmentCfg;
  }

  @Override
  public RemoteEnvironmentCfg realClusterCfg() {
    return remoteEnvironmentCfg;
  }
}
