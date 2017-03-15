package com.spbsu.datastream.core.routing;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.materializer.AddressedMessage;
import scala.Option;

import java.util.HashMap;
import java.util.Map;

import static com.spbsu.datastream.core.routing.RootRouterApi.RegisterMe;

public class RootRouter extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private final HashRange range;
  private final ActorRef remoteRouter;
  private final Map<Long, ActorRef> tickLocalRouters = new HashMap<>();
  private final Map<ActorRef, Long> reverseTickLocalRouters = new HashMap<>();

  private RootRouter(final HashRange range, final ActorRef remoteRouter) {
    this.range = range;
    this.remoteRouter = remoteRouter;
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(RootRouter.class, range, remoteRouter);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      route((AddressedMessage) message);
    } else if (message instanceof RegisterMe) {
      register((RegisterMe) message);
    } else if (message instanceof Terminated) {
      unregister((Terminated) message);
    } else {
      unhandled(message);
    }
  }

  private void route(final AddressedMessage<?> addressedMessage) {
    LOG.debug("Routing of {}", addressedMessage);
    if (addressedMessage.payload().isBroadcast()) {
      LOG.debug("Broadcast routing of {}", addressedMessage);
      remoteRouter.tell(addressedMessage, self());
    } else if (range.isIn(addressedMessage.payload().hash())) {
      LOG.debug("Local routing of {}", addressedMessage);
      routeLocal(addressedMessage);
    } else {
      LOG.debug("Remote routing of {}", addressedMessage);
      remoteRouter.tell(addressedMessage, self());
    }
  }

  private void routeLocal(final AddressedMessage<?> message) {
    final long tick = message.payload().meta().tick();
    final ActorRef dest = tickLocalRouters.getOrDefault(tick, context().system().deadLetters());
    dest.tell(message, self());
  }

  private void register(final RegisterMe registerMe) {
    tickLocalRouters.putIfAbsent(registerMe.tick(), registerMe.actorRef());
    reverseTickLocalRouters.putIfAbsent(registerMe.actorRef(), registerMe.tick());
    context().watch(registerMe.actorRef());
    LOG.info("Registered. Tick: {}, actor: {}", registerMe.tick(), registerMe.actorRef());
  }

  private void unregister(final Terminated terminated) {
    final long tick = reverseTickLocalRouters.get(terminated.actor());
    reverseTickLocalRouters.remove(terminated.actor());
    tickLocalRouters.remove(tick);
    LOG.info("Unregistered. Tick: {}, actor: {}", tick, terminated.actor());
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");
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
}
