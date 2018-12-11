package com.spbsu.flamestream.runtime.master.acker.api.commit;

import akka.actor.ActorRef;

public class MinTimeUpdateListener {
  public final ActorRef actorRef;

  public MinTimeUpdateListener(ActorRef actorRef) {
    this.actorRef = actorRef;
  }
}
