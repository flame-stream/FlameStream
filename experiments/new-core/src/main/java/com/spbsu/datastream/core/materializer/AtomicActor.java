package com.spbsu.datastream.core.materializer;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.DataItem;
import scala.Option;

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
    LOG.info("Starting...");
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) {
    LOG.warning("Restarting, reason: {}, message: {}", reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    LOG.debug("Recived {}", message);

    if (message instanceof TickGraphManagerApi.Start) {
      logic.onStart();
    } else if (message instanceof AddressedMessage) {
      final AddressedMessage m = (AddressedMessage) message;
      if (m.payload() instanceof DataItem) {
        //noinspection unchecked
        logic.onPush(m.port(), ((DataItem) m.payload()));
      }
    } else {
      unhandled(message);
    }
  }
}
