package com.spbsu.flamestream.runtime.edge.socket;

import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class SocketFrontType implements FlameRuntime.FrontType<SocketFront, Void> {
  private final String host;
  private final int port;
  private final Class<?>[] inputClasses;

  public SocketFrontType(String host, int port, Class<?>... inputClasses) {
    this.host = host;
    this.port = port;
    this.inputClasses = inputClasses;
  }

  @Override
  public FlameRuntime.FrontInstance<SocketFront> instance() {
    return new SocketFrontFrontInstance(host, port, inputClasses);
  }

  @Override
  public Void handle(EdgeContext context) {
    return null;
  }

  private static class SocketFrontFrontInstance implements FlameRuntime.FrontInstance<SocketFront> {
    private final String host;
    private final int port;
    private final Class<?>[] inputClasses;

    private SocketFrontFrontInstance(String host, int port, Class<?>[] inputClasses) {
      this.host = host;
      this.port = port;
      this.inputClasses = inputClasses;
    }

    @Override
    public Class<SocketFront> clazz() {
      return SocketFront.class;
    }

    @Override
    public Object[] params() {
      return new Object[] {host, port, inputClasses};
    }
  }
}
