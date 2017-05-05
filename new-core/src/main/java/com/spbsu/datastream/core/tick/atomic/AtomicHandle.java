package com.spbsu.datastream.core.tick.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;

import java.util.Optional;

public interface AtomicHandle {
  void push(OutPort out, DataItem<?> result);

  void ack(DataItem<?> item);

  Optional<Object> loadState(InPort inPort);

  void saveState(InPort inPort, Object state);

  void removeState(InPort inPort);

  HashRange localRange();
}

