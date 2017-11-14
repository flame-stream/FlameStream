package com.spbsu.flamestream.core.graph.source.impl;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.atomic.impl.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.source.Source;
import com.spbsu.flamestream.core.graph.source.SourceHandle;

import java.util.Collections;
import java.util.List;

/**
 * User: Artem
 * Date: 14.11.2017
 */
public abstract class AbstractSource extends AbstractAtomicGraph implements Source {
  protected final OutPort outPort = new OutPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }

  public OutPort outPort() {
    return this.outPort;
  }

  @Override
  public void onHeartbeat(GlobalTime globalTime, SourceHandle handle) {
    handle.heartbeat(globalTime);
  }
}
