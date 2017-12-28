package com.spbsu.flamestream.runtime.edge.socket;

import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class SocketFrontType implements FlameRuntime.FrontType<SocketFront, SocketFrontType.Handle> {
  private final String host;
  private final int port;

  public SocketFrontType(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public FlameRuntime.FrontInstance<SocketFront> instance() {
    return new FlameRuntime.FrontInstance<SocketFront>() {
      @Override
      public Class<SocketFront> clazz() {
        return SocketFront.class;
      }

      @Override
      public Object[] params() {
        return new Object[] {host, port};
      }
    };
  }

  @Override
  public Handle handle(EdgeContext context) {
    return new Handle();
  }

  public static class Handle {
    
  }
}
