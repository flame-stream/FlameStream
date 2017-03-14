package com.spbsu.datastream.core.materializer.routing;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.materializer.AddressedMessage;

public class ForkRouter extends UntypedActor {
  private final HashRange range;

  private final ActorRef remoteRouter;

  private ActorRef localRouter;

  private ForkRouter(final HashRange range, final ActorRef remoteRouter) {
    this.range = range;
    this.remoteRouter = remoteRouter;
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(ForkRouter.class, range, remoteRouter);
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

        if (range.isIn(di.hash())) {
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
    } else {
      unhandled(message);
    }
  }
}
