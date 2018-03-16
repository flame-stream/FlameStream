package com.spbsu.flamestream.runtime.edge.socket;

import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

/**
 * User: Artem
 * Date: 29.12.2017
 */
public class SocketRearType implements FlameRuntime.RearType<SocketRear, Void> {
  private final String host;
  private final int port;
  private final Class<?>[] outClasses;

  public SocketRearType(String host, int port, Class<?>... outClasses) {
    this.host = host;
    this.port = port;
    this.outClasses = outClasses;
  }

  @Override
  public FlameRuntime.RearInstance<SocketRear> instance() {
    return new SocketRearRearInstance(host, port, outClasses);
  }

  @Override
  public Void handle(EdgeContext context) {
    return null;
  }

  private static class SocketRearRearInstance implements FlameRuntime.RearInstance<SocketRear> {
    private final String host;
    private final int port;
    private final Class<?>[] outClasses;

    private SocketRearRearInstance(String host, int port, Class<?>[] outClasses) {
      this.host = host;
      this.port = port;
      this.outClasses = outClasses;
    }

    @Override
    public Class<SocketRear> clazz() {
      return SocketRear.class;
    }

    @Override
    public Object[] params() {
      return new Object[] {host, port, outClasses};
    }
  }
}
