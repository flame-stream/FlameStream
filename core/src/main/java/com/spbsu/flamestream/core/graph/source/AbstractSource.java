package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.Collections;
import java.util.List;

public abstract class AbstractSource extends AbstractAtomicGraph implements Source {
  private final OutPort outPort = new OutPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public void onHeartbeat(GlobalTime globalTime, SourceHandle handle) {
    handle.heartbeat(globalTime);
  }
}
