package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.node.DeployForTick;
import com.spbsu.datastream.core.tick.TickConcierge;
import com.spbsu.datastream.core.tick.TickContext;
import com.spbsu.datastream.core.tick.TickContextImpl;

public final class RangeConcierge extends LoggingActor {
  private final HashRange myRange;

  private final ActorRef rangeRouter;
  private final ActorRef rootRouter;

  private RangeConcierge(final HashRange myRange, final ActorRef rootRouter) {
    this.myRange = myRange;
    this.rootRouter = rootRouter;

    this.rangeRouter = this.context().actorOf(RangeRouter.props(), "rangeRouter");
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(RangeConcierge.class, range, remoteRouter);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received: {}", message);

    if (message instanceof DeployForTick) {
      final DeployForTick deploy = (DeployForTick) message;
      this.context().actorOf(
              TickConcierge.props(this.tickContext(deploy)),
              Long.toString(deploy.tick()));
    } else {
      this.unhandled(message);
    }
  }

  private TickContext tickContext(final DeployForTick deployForTick) {
    return new TickContextImpl(this.rootRouter,
            this.rangeRouter,
            deployForTick.graph(),
            deployForTick.tick(),
            deployForTick.startTs(), deployForTick.window(),
            this.myRange,
            deployForTick.ackerRange());
  }
}
