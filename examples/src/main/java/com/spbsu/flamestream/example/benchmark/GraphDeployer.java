package com.spbsu.flamestream.example.benchmark;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public interface GraphDeployer extends AutoCloseable {
  class Handle {
    final InetSocketAddress front, rear;

    public Handle(InetSocketAddress front, InetSocketAddress rear) {
      this.front = front;
      this.rear = rear;
    }
  }

  Map<String, Handle> deploy();

  @Override
  void close();
}
