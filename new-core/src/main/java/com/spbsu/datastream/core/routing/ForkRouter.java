package com.spbsu.datastream.core.routing;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.materializer.AddressedMessage;
import scala.Option;

public class ForkRouter extends UntypedActor {
  final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private final HashRange localRange;

  private final ActorRef remoteRouter;

  private ActorRef localRouter;

  private ForkRouter(final HashRange localRange, final ActorRef remoteRouter) {
    this.localRange = localRange;
    this.remoteRouter = remoteRouter;
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(ForkRouter.class, range, remoteRouter);
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    waitingForLocalRouter(message);
  }

  private void servingRequests(final Object message) {
    if (message instanceof AddressedMessage) {
      final AddressedMessage<?> addressedMessage = (AddressedMessage<?>) message;
      if (addressedMessage.payload() instanceof DataItem) {
        final DataItem<?> di = (DataItem<?>) addressedMessage.payload();

        if (localRange.isIn(di.hash())) {
          localRouter.tell(message, self());
        } else {
          remoteRouter.tell(message, self());
        }
      } else {
        localRouter.tell(message, self());
        remoteRouter.tell(message, self());
      }
    } else {
      unhandled(message);
    }
  }

  private void waitingForLocalRouter(final Object message) {
    if (message instanceof ActorRef) {
      this.localRouter = (ActorRef) message;
      getContext().become(this::servingRequests);
      LOG.info("Ready to handle messages");
    } else {
      unhandled(message);
    }
  }
}
