package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.graph.InPort;

/**
 * Created by marnikitta on 2/8/17.
 */
public interface PortBindingApi {
  class MessageFromPort<T> {
    private final InPort port;

    private final T payload;

    public MessageFromPort(final T payload, final InPort port) {
      this.port = port;
      this.payload = payload;
    }

    public InPort port() {
      return port;
    }

    public T payload() {
      return payload;
    }

    @Override
    public String toString() {
      return "MessageFromPort{" + "port=" + port +
              ", payload=" + payload +
              '}';
    }
  }
}
