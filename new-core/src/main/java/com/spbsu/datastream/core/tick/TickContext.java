package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.node.TickInfo;

public interface TickContext {
  TickInfo tickInfo();

  HashRange localRange();

  ActorRef rootRouter();

  ActorRef rangeRouter();
}
