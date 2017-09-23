package com.spbsu.flamestream.benchmarks.config;

import com.typesafe.config.Config;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class TypesafeClusterCfg implements ClusterCfg {
  private final boolean local;
  private final LocalClusterCfg localClusterCfg;
  private final RealClusterCfg realClusterCfg;

  public TypesafeClusterCfg(Config load) {
    final Config config = load.getConfig("cluster");
    this.local = config.getBoolean("local");
    this.localClusterCfg = new LocalClusterCfg(config.getInt("local-cluster.workers"), config.getInt("local-cluster.workers"));
    this.realClusterCfg = new RealClusterCfg(
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
  public LocalClusterCfg localClusterCfg() {
    return localClusterCfg;
  }

  @Override
  public RealClusterCfg realClusterCfg() {
    return realClusterCfg;
  }
}
