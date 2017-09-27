package com.spbsu.flamestream.config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface EnvironmentCfg {
  boolean isLocal();

  LocalEnvironmentCfg localClusterCfg();

  RemoteEnvironmentCfg realClusterCfg();
}