package com.spbsu.flamestream.runtime.node.graph.materialization;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.OutPort;

public interface GraphRouter {
  void tell(DataItem<?> message, Graph.Vertex<?> destanation, ActorRef sender);

  void broadcast(Object message, ActorRef sender);
}
