package com.spbsu.datastream.core;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.job.control.Control;

public class ActorSink implements Sink {

  ActorRef actor;

  public ActorSink(ActorRef actor) {
    this.actor = actor;
  }

  @Override
  public void accept(DataItem item) {
    actor.tell(item, null);
  }

  @Override
  public void accept(Control control) {
    actor.tell(control, null);
  }
}
