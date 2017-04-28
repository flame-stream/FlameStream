package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.tick.TickContext;
import com.spbsu.datastream.core.tick.TickContextImpl;
import com.spbsu.datastream.core.tick.TickConcierge;

import static com.spbsu.datastream.core.range.RangeConciergeApi.DeployForTick;

public final class RangeConcierge extends LoggingActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());
  private final HashRange range;

  private final ActorRef rangeRouter;

  private final ActorRef rootRouter;

  private RangeConcierge(final HashRange range, final ActorRef rootRouter) {
    this.range = range;
    this.rootRouter = rootRouter;

    this.rangeRouter = this.context().actorOf(RangeRouter.props(), "rangeRouter");
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(RangeConcierge.class, range, remoteRouter);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG.debug("Received: {}", message);

    if (message instanceof DeployForTick) {
      final DeployForTick deploy = (DeployForTick) message;
      this.context().actorOf(
              TickConcierge.props(this.tickContext(deploy.tick(), deploy.graph(), deploy.startTs(), deploy.window())),
              Long.toString(deploy.tick()));
    } else {
      this.unhandled(message);
    }
  }

  private TickContext tickContext(final int tick, final TheGraph graph, final long startTs, final long window) {
    return new TickContextImpl(this.rootRouter, this.rangeRouter, tick, this.range, startTs, window, graph);
  }
}
