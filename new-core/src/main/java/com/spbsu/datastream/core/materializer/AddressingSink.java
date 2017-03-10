package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.materializer.atomic.Control;
import com.spbsu.datastream.core.materializer.atomic.DataSink;

public class AddressingSink implements DataSink {
  private final ActorRef actorRef;
  private final InPort address;

  public AddressingSink(final ActorRef actorRef, final InPort address) {
    this.actorRef = actorRef;
    this.address = address;
  }

  @Override
  public void accept(final DataItem dataItem) {
    actorRef.tell(new AddressedMessage<>(dataItem, address), null);
  }

  @Override
  public void accept(final Control control) {
    actorRef.tell(new AddressedMessage<>(control, address), null);
  }
}
