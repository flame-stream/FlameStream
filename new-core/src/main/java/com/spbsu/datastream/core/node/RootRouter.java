package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.tick.AddressedMessage;
import scala.Option;

import java.util.HashMap;
import java.util.Map;

import static com.spbsu.datastream.core.node.RootRouterApi.RegisterMe;

public final class RootRouter extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private final HashRange range;
  private final ActorRef remoteRouter;
  private final Map<Long, ActorRef> tickLocalRouters = new HashMap<>();
  private final Map<ActorRef, Long> reverseTickLocalRouters = new HashMap<>();

  private RootRouter(final HashRange range, final ActorRef remoteRouter) {
    super();
    this.range = range;
    this.remoteRouter = remoteRouter;
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(RootRouter.class, range, remoteRouter);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    this.LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    this.LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @SuppressWarnings({"IfStatementWithTooManyBranches", "ChainOfInstanceofChecks"})
  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      this.route((AddressedMessage) message);
    } else if (message instanceof RegisterMe) {
      this.register((RegisterMe) message);
    } else if (message instanceof Terminated) {
      this.unregister((Terminated) message);
    } else {
      this.unhandled(message);
    }
  }

  private void route(final AddressedMessage addressedMessage) {
    this.LOG.debug("Routing of {}", addressedMessage);

    if (addressedMessage.isBroadcast()) {
      this.LOG.debug("Broadcast routing of {}", addressedMessage);

      this.remoteRouter.tell(addressedMessage, this.self());
    } else if (this.range.isIn(addressedMessage.hash())) {
      this.LOG.debug("Local routing of {}", addressedMessage);

      this.routeLocal(addressedMessage);
    } else {
      this.LOG.debug("Remote routing of {}", addressedMessage);

      this.remoteRouter.tell(addressedMessage, this.self());
    }
  }

  private void routeLocal(final AddressedMessage message) {
    final long tick = message.payload().meta().tick();
    final ActorRef dest = this.tickLocalRouters.getOrDefault(tick, this.context().system().deadLetters());
    dest.tell(message, this.self());
  }

  private void register(final RegisterMe registerMe) {
    this.tickLocalRouters.putIfAbsent(registerMe.tick(), registerMe.actorRef());
    this.reverseTickLocalRouters.putIfAbsent(registerMe.actorRef(), registerMe.tick());
    this.context().watch(registerMe.actorRef());
    this.LOG.info("Registered. Tick: {}, actor: {}", registerMe.tick(), registerMe.actorRef());
  }

  private void unregister(final Terminated terminated) {
    final long tick = this.reverseTickLocalRouters.get(terminated.actor());
    this.reverseTickLocalRouters.remove(terminated.actor());
    this.tickLocalRouters.remove(tick);
    this.LOG.info("Unregistered. Tick: {}, actor: {}", tick, terminated.actor());
  }
}
