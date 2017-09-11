package com.spbsu.datastream.benchmarks;

import com.spbsu.datastream.core.Cluster;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface ClusterRunner {
  void run(Cluster cluster) throws InterruptedException;
}
