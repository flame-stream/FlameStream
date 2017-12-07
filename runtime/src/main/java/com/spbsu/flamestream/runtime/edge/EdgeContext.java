package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import com.spbsu.flamestream.core.data.meta.EdgeInstance;

public interface EdgeContext {
  ActorPath nodePath();

  EdgeInstance edgeInstance();
}
