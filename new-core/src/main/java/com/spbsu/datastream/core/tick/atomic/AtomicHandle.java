package com.spbsu.datastream.core.tick.atomic;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ops.GroupingState;
import sun.security.krb5.internal.APOptions;

import java.util.Optional;

public interface AtomicHandle {
  ActorSelection actorSelection(ActorPath path);

  void push(OutPort out, DataItem<?> result);

  void ack(DataItem<?> item);

  Optional<Object> loadState(InPort inPort);

  void saveState(InPort inPort, Object state);

  void removeState(InPort inPort);

  HashRange localRange();
}

