package com.spbsu.flamestream.runtime.node.front;

import akka.actor.ActorPath;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.NewHole;

/**
 * User: Artem
 * Date: 14.11.2017
 */
public class MediatorFront extends LoggingActor {
  private final ActorPath realFrontPath;

  public MediatorFront(ActorPath realFrontPath) {
    this.realFrontPath = realFrontPath;
  }

  public static Props props(ActorPath realFront) {
    return Props.create(MediatorFront.class, realFront);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(NewHole.class, this::onNewHole)
            .build();
  }

  private void onNewHole(NewHole hole) {
    context().actorSelection(realFrontPath).tell(hole, sender());
  }
}
