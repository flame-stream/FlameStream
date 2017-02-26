package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.spbsu.datastream.core.graph.InPort;

/**
 * Created by marnikitta on 2/8/17.
 */
public class PortListener extends UntypedActor {
  private final ActorRef processor;
  private final InPort port;

  public static Props props(final ActorRef processor, final InPort port) {
    return Props.create(PortListener.class, new Creator<PortListener>() {
      @Override
      public PortListener create() throws Exception {
        return new PortListener(processor, port);
      }
    });
  }

  private PortListener(final ActorRef processor, final InPort port) {
    this.processor = processor;
    this.port = port;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    processor.tell(new PortBindingApi.MessageFromPort<>(message, port), getSender());
  }
}
