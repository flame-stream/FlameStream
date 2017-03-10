package com.spbsu.datastream.core.materializer.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.locator.PortLocator;

public class AtomicHandleImpl implements AtomicHandle {
  private final PortLocator portLocator;

  public AtomicHandleImpl(final PortLocator portLocator) {
    this.portLocator = portLocator;
  }

  @Override
  public void push(final OutPort out, final DataItem result) {
    portLocator.sinkForPort(out).orElseThrow(RuntimeException::new).accept(result);
  }

  @Override
  public void panic(final Exception e) {
    throw new RuntimeException(e);
  }
}
