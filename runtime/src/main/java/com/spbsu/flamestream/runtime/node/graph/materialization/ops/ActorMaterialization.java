package com.spbsu.flamestream.runtime.node.graph.materialization.ops;

import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class ActorMaterialization extends LoggingActor {
  private final Materialization inner;

  public ActorMaterialization(Materialization inner) {
    this.inner = inner;
  }

  @Override
  public Receive createReceive() {
    return null;
  }
}
