package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;

import java.util.Map;

public interface TickContext {
  Map<OutPort, InPort> downstreams();

  ActorRef forkRouter();
}
