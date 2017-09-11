package com.spbsu.datastream.benchmarks.config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class LocalClusterCfg {
  private final int workers;
  private final int fronts;

  LocalClusterCfg(int workers, int fronts) {
    this.workers = workers;
    this.fronts = fronts;
  }

  public int workers() {
    return workers;
  }

  public int fronts() {
    return fronts;
  }
}
