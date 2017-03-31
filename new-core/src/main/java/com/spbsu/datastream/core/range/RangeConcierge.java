package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.tick.TickContext;
import com.spbsu.datastream.core.tick.TickContextImpl;
import com.spbsu.datastream.core.tick.manager.TickGraphManager;
import com.spbsu.datastream.core.node.RootRouter;
import scala.Option;

import static com.spbsu.datastream.core.range.RangeConciergeApi.DeployForTick;

public class RangeConcierge extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());
  private final HashRange range;

  private final ActorRef rootRouter;

  private RangeConcierge(final HashRange range, final ActorRef remoteRouter) {
    this.range = range;
    this.rootRouter = context().actorOf(RootRouter.props(range, remoteRouter), "rootRouter");
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(RangeConcierge.class, range, remoteRouter);
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
    LOG.debug("Received: {}", message);

    if (message instanceof DeployForTick) {
      final DeployForTick deploy = (DeployForTick) message;
      context().actorOf(
              TickGraphManager.props(tickContext(deploy.tick(), deploy.graph())),
              Long.toString(deploy.tick()));
    } else {
      unhandled(message);
    }
  }

  private TickContext tickContext(final long tick, final TheGraph graph) {
    return new TickContextImpl(this.rootRouter, tick, this.range, graph);
  }
}
