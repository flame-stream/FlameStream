package com.spbsu.flamestream.benchmark.config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class LocalEnvironmentCfg {
  private final int workers;
  private final int fronts;

  LocalEnvironmentCfg(int workers, int fronts) {
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
