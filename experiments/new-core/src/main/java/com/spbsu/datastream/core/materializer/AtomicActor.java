package com.spbsu.datastream.core.materializer;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.DataItem;

public class AtomicActor extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), getSelf());

  private final GraphStageLogic logic;

  public static Props props(final GraphStageLogic logic) {
    return Props.create(AtomicActor.class, logic);
  }

  private AtomicActor(final GraphStageLogic logic) {
    this.logic = logic;
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Atomic actor is starting {}", logic);
    super.preStart();
    logic.onStart();
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      final AddressedMessage m = (AddressedMessage) message;
      if (m.payload() instanceof DataItem) {
        logic.onPush(m.port(), ((DataItem) m.payload()));
      }
    }
  }
}
