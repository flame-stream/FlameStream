package com.spbsu.flamestream.example.benchmark;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public interface GraphDeployer extends AutoCloseable {
  void deploy();

  @Override
  void close();
}
