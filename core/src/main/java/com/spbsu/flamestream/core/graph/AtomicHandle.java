package com.spbsu.flamestream.core.graph;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.stat.Statistics;
import com.spbsu.flamestream.core.TickInfo;

public interface AtomicHandle {
  void push(OutPort out, DataItem<?> result);

  void ack(DataItem<?> item);

  void submitStatistics(Statistics stat);

  TickInfo tickInfo();

  void error(String format, Object... args);
}

