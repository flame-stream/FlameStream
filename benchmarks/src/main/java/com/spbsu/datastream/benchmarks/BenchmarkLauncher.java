package com.spbsu.datastream.benchmarks;

import com.spbsu.datastream.benchmarks.bl.wordcount.RealCluster;
import com.spbsu.datastream.benchmarks.config.ClusterCfg;
import com.spbsu.datastream.benchmarks.config.ClusterRunnerCfg;
import com.spbsu.datastream.benchmarks.config.TypesafeClusterCfg;
import com.spbsu.datastream.benchmarks.config.TypesafeClusterRunnerCfg;
import com.spbsu.datastream.core.Cluster;
import com.spbsu.datastream.core.LocalCluster;
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
      load = ConfigFactory.load();
    }

    final ClusterCfg clusterCfg = new TypesafeClusterCfg(load);
    final ClusterRunnerCfg clusterRunnerCfg = new TypesafeClusterRunnerCfg(load);

    final Cluster cluster;
    if (clusterCfg.isLocal()) {
      cluster = new LocalCluster(clusterCfg.localClusterCfg().workers(), clusterCfg.localClusterCfg().fronts());
    } else {
      cluster = new RealCluster(clusterCfg.realClusterCfg().zkString(), clusterCfg.realClusterCfg().nodes(), clusterCfg.realClusterCfg().fronts());
    }

    final ClusterRunner runner = clusterRunnerCfg.runner().newInstance();
    runner.run(cluster);
  }
}
