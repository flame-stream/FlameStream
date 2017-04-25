package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

import java.util.Set;

public interface TickContext {
  TheGraph graph();

  long tick();

  long startTime();

  long window();

  HashRange localRange();

  ActorRef rootRouter();
}
