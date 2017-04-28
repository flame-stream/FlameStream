package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

public interface TickContext {
  TheGraph graph();

  int tick();

  long startTime();

  long window();

  HashRange localRange();

  ActorRef rootRouter();

  ActorRef rangeRouter();
}
