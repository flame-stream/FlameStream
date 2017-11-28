package com.spbsu.flamestream.runtime.node.graph.materialization;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;

public interface GraphRouter {
  void tell(DataItem<?> message, Graph.Vertex destanation, ActorRef sender);

  void broadcast(Object message, ActorRef sender);
}
