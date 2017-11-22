package com.spbsu.flamestream.runtime.node.materializer.router;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.OutPort;

public interface FlameRouter {
  void tell(DataItem<?> message, OutPort source, ActorRef sender);

  void broadcast(Object message, ActorRef sender);
}
