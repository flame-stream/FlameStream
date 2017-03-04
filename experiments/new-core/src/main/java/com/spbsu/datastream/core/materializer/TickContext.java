package com.spbsu.datastream.core.materializer;

public interface TickContext {
  default PortLocator portLocator() {
    return localLocator();
  }

  LocalPortLocator localLocator();

  RemotePortLocator remoteLocator();
}
