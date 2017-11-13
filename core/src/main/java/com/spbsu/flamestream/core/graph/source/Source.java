package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Source extends AbstractAtomicGraph {
  private static final int MAX_IN_FLIGHT_ITEMS = 10;

  private final OutPort outPort = new OutPort();
  private final List<GlobalTime> inFlight = new ArrayList<>(MAX_IN_FLIGHT_ITEMS);

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

  public void onMinGTimeUpdate(GlobalTime globalTime, SourceHandle handle) {
    // TODO: 10.11.2017 analyze performance
    /*final Iterator<GlobalTime> iterator = inFlight.iterator();
    while (iterator.hasNext()) {
      final GlobalTime next = iterator.next();
      if (next.compareTo(globalTime) < 0) {
        handle.accept(next);
        iterator.remove();
      }
    }*/
  }

  public void onNext(DataItem<?> item, SourceHandle handle) {
    handle.push(outPort, item);
    handle.ack(item.ack(), item.meta().globalTime());

    final GlobalTime time = item.meta().globalTime();
    handle.accept(time);

    /*inFlight.add(time);
    if (inFlight.size() < MAX_IN_FLIGHT_ITEMS) {
      handle.accept(time);
    } else if (inFlight.size() > MAX_IN_FLIGHT_ITEMS) {
      throw new IllegalStateException("Limit of in flight items is exceeded");
    }*/
  }

  public void onHeartbeat(GlobalTime globalTime, SourceHandle handle) {
    handle.heartbeat(globalTime);
  }
}
