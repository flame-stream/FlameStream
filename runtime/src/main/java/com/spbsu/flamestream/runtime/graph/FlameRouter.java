package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import com.spbsu.flamestream.runtime.graph.materialization.api.AddressedItem;

public interface FlameRouter {
  void tell(AddressedItem item, ActorRef sender);

  void broadcast(Object message, ActorRef sender);
}
