package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BackPressureSource extends AbstractSource {
  // TODO: 14.11.2017 tune magic constant in runtime
  private static final int MAX_IN_FLIGHT_ITEMS = 10;
  private final List<InFlightElement> inFlight = new ArrayList<>();

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    final SourceHandle sourceHandle = (SourceHandle) handle;
    final Iterator<InFlightElement> iterator = inFlight.iterator();
    while (iterator.hasNext()) {
      final InFlightElement next = iterator.next();
      final GlobalTime nextTime = next.globalTime;
      if (nextTime.compareTo(globalTime) <= 0) {
        iterator.remove();
        if (!next.accepted) {
          sourceHandle.accept(nextTime);
        }
      }
    }
  }

  @Override
  public void onNext(DataItem<?> item, SourceHandle handle) {
    handle.push(outPort(), item);
    handle.ack(item.xor(), item.meta().globalTime());

    if (inFlight.size() < MAX_IN_FLIGHT_ITEMS) {
      handle.accept(item.meta().globalTime());
      inFlight.add(new InFlightElement(item.meta().globalTime(), true));
    } else {
      handle.heartbeat(item.meta().globalTime());
      inFlight.add(new InFlightElement(item.meta().globalTime(), false));
    }
  }

  private static class InFlightElement {
    private final GlobalTime globalTime;
    private final boolean accepted;

    private InFlightElement(GlobalTime globalTime, boolean accepted) {
      this.globalTime = globalTime;
      this.accepted = accepted;
    }
  }
}
