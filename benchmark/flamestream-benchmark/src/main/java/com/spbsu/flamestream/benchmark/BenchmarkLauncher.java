package com.spbsu.flamestream.benchmark;

import com.spbsu.flamestream.benchmark.config.ClusterRunnerCfg;
import com.spbsu.flamestream.benchmark.config.TypesafeClusterRunnerCfg;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class BenchmarkLauncher {
  public static void main(String[] args) throws Exception {
    final Config load;
    if (args.length == 1) {
      final Path filename = Paths.get(args[0]);
      load = ConfigFactory.parseReader(Files.newBufferedReader(filename));
    } else {
      load = ConfigFactory.load("bench");
    }

    final ClusterRunnerCfg clusterRunnerCfg = new TypesafeClusterRunnerCfg(load);
    /*final EnvironmentRunner runner = clusterRunnerCfg.runner().newInstance();
    final EnvironmentCfg clusterCfg = new TypesafeEnvironmentCfg(load);
    if (clusterCfg.isLocal()) {
      try (final Cluster cluster = new LocalCluster(clusterCfg.localClusterCfg().workers(), clusterCfg.localClusterCfg().fronts())) {
        runner.run(cluster);
      }
    } else {
      try (final Cluster cluster = new RealCluster(clusterCfg.realClusterCfg().zkString(), clusterCfg.realClusterCfg().nodes(), clusterCfg.realClusterCfg().fronts())) {
        runner.run(cluster);
      }
    }*/
  }
}
