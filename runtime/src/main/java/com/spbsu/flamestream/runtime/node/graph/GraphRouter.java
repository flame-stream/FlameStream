package com.spbsu.flamestream.runtime.node.graph;

import akka.actor.ActorRef;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.AddressedItem;

public interface GraphRouter {
  void tell(AddressedItem item, ActorRef sender);

  void broadcast(Object message, ActorRef sender);
}
