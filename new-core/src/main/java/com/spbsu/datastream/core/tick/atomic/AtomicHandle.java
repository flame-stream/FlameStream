package com.spbsu.datastream.core.tick.atomic;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ops.GroupingState;
import sun.security.krb5.internal.APOptions;

public interface AtomicHandle {
  ActorSelection actorSelection(ActorPath path);

  void push(OutPort out, DataItem<?> result);

  void ack(DataItem<?> item);

  GroupingState<?> loadGroupingState();

  void saveGroupingState(GroupingState<?> storage);

  HashRange localRange();
}

