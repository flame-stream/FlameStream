package com.spbsu.flamestream.benchmarks;

import com.spbsu.flamestream.core.Cluster;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface ClusterRunner {
  void run(Cluster cluster) throws InterruptedException;
}
