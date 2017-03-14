package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.materializer.TickContext;
import com.spbsu.datastream.core.materializer.manager.TickGraphManager;

import static com.spbsu.datastream.core.deploy.DeployApi.DeployForTick;

public class RangeConcierge extends UntypedActor {
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
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof DeployForTick) {
      final DeployForTick deploy = (DeployForTick) message;
      final ActorRef tickManager = getContext().actorOf(TickGraphManager.props(remoteRouter, deploy.graph()));
    }
  }

  public TickContext contextForTick() {
    throw new UnsupportedOperationException();
  }
}
