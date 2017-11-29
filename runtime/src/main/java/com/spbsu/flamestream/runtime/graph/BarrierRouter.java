package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;

public interface BarrierRouter {
  void emmit(DataItem<?> item, ActorRef sender);
}
