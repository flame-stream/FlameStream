package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import com.spbsu.flamestream.core.data.meta.EdgeId;

public interface EdgeContext {
  ActorPath nodePath();

  EdgeId edgeId();
}
