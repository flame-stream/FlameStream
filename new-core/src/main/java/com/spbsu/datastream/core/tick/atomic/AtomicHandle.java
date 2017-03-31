package com.spbsu.datastream.core.tick.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.TheGraph;

public interface AtomicHandle {
  void deploy(TheGraph graph);

  void push(OutPort out, DataItem<?> result);

  void panic(Exception e);

  /**
   * Inspired by Apache Storm
   */
  void ack(InPort port, DataItem<?> dataItem);

  void fail(DataItem<?> dataItem, InPort inPort, Exception reason);
}

