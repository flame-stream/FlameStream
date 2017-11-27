package com.spbsu.flamestream.runtime.node.graph.router;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.OutPort;

public interface FlameRouter {
  void tell(DataItem<?> message, OutPort source, ActorRef sender);
}
