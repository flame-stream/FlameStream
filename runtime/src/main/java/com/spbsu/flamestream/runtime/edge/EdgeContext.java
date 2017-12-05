package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;

public interface EdgeContext {
  ActorPath nodePath();

  String nodeId();

  String edgeId();
}
