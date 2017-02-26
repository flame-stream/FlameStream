package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;

/**
 * Created by marnikitta on 2/8/17.
 */
public interface PortLocatorApi {
  class Locate {
    private final OutPort outPort;

    public Locate(final OutPort outPort) {
      this.outPort = outPort;
    }

    public OutPort outPort() {
      return outPort;
    }

    @Override
    public String toString() {
      return "Locate{" + "outPort=" + outPort +
              '}';
    }
  }

  class LocationResponse {
    private final OutPort outPort;

    private final InPort inPort;

    private final ActorRef ref;

    public LocationResponse(final OutPort outPort, final InPort inPort, final ActorRef ref) {
      this.outPort = outPort;
      this.inPort = inPort;
      this.ref = ref;
    }

    public OutPort outPort() {
      return outPort;
    }

    public InPort inPort() {
      return inPort;
    }

    public ActorRef ref() {
      return ref;
    }

    @Override
    public String toString() {
      return "LocationResponse{" + "outPort=" + outPort +
              ", inPort=" + inPort +
              ", ref=" + ref +
              '}';
    }
  }
}
