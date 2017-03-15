package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.materializer.manager.TickGraphManager;
import scala.Option;

import static com.spbsu.datastream.core.deploy.DeployApi.DeployForTick;

public class RangeConcierge extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());
  private final HashRange range;

  private final ActorRef remoteRouter;

  private RangeConcierge(final HashRange range, final ActorRef remoteRouter) {
    this.range = range;
    this.remoteRouter = remoteRouter;
  }

  public static Props props(final HashRange range, final ActorRef remoteRouter) {
    return Props.create(RangeConcierge.class, range, remoteRouter);
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting... Range: {}", range);
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
              TickGraphManager.props(remoteRouter, range, deploy.graph()),
              Long.toString(deploy.tick()));
    } else {
      unhandled(message);
    }
  }
}
