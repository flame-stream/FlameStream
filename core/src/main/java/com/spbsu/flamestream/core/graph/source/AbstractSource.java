package com.spbsu.flamestream.core.graph.source.impl;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.source.Source;
import com.spbsu.flamestream.core.graph.source.SourceHandle;

import java.util.Collections;
import java.util.List;

/**
 * User: Artem
 * Date: 14.11.2017
 */
public abstract class AbstractSource extends AbstractAtomicGraph implements Source {
  // TODO: 14.11.2017 tune magic constant in runtime
  private static final int MAX_ITEMS_BETWEEN_HEARTBEATS = 100;

  protected final OutPort outPort = new OutPort();
  private int itemsAfterPrevHeartbeat = 0;

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
  public void onNext(DataItem<?> item, SourceHandle handle) {
    itemsAfterPrevHeartbeat++;
    if (itemsAfterPrevHeartbeat == MAX_ITEMS_BETWEEN_HEARTBEATS) {
      handle.heartbeat(item.meta().globalTime());
      itemsAfterPrevHeartbeat = 0;
    }
  }

  @Override
  public void onHeartbeat(GlobalTime globalTime, SourceHandle handle) {
    handle.heartbeat(globalTime);
    itemsAfterPrevHeartbeat = 0;
  }
}
