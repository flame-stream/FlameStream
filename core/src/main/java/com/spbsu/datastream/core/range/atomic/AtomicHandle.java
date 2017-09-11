package com.spbsu.datastream.core.range.atomic;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.stat.Statistics;
import com.spbsu.datastream.core.tick.TickInfo;

public interface AtomicHandle {
  ActorSelection actorSelection(ActorPath path);

  void push(OutPort out, DataItem<?> result);

  void ack(DataItem<?> item);

  void submitStatistics(Statistics stat);

  TickInfo tickInfo();

  void error(String format, Object... args);
}

