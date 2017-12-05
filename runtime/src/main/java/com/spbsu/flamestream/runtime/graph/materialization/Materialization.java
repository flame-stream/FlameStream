package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public interface Materialization {
  void input(DataItem dataItem, ActorRef front);

  void inject(Materializer.Destination destination, DataItem dataItem);

  void minTime(GlobalTime minTime);

  void commit();
}
