package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

public interface TickContext {
  TheGraph graph();

  long tick();

  HashRange localRange();

  ActorRef rootRouter();
}
