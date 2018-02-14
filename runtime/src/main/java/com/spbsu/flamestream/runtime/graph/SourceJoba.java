package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

public class SourceJoba implements Joba {
  private final Collection<InFlightTime> inFlight = new ArrayList<>();
  private final Map<EdgeId, ActorRef> fronts = new HashMap<>();
  private final int maxInFlightItems;
  private final ActorContext context;

  public SourceJoba(int maxInFlightItems, ActorContext context) {
    this.maxInFlightItems = maxInFlightItems;
    this.context = context;
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    sink.accept(item);
    { //back-pressure logic
      final GlobalTime globalTime = item.meta().globalTime();
      if (inFlight.size() < maxInFlightItems) {
        fronts.get(globalTime.frontId()).tell(new RequestNext(), context.self());
        inFlight.add(new InFlightTime(globalTime, true));
      } else {
        inFlight.add(new InFlightTime(globalTime, false));
      }
    }
  }


  public void addFront(EdgeId front, ActorRef actorRef) {
    fronts.putIfAbsent(front, actorRef);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    final Iterator<InFlightTime> iterator = inFlight.iterator();
    while (iterator.hasNext()) {
      final InFlightTime next = iterator.next();
      final GlobalTime nextTime = next.globalTime;
      if (nextTime.compareTo(minTime) <= 0) {
        iterator.remove();
        if (!next.accepted) {
          fronts.get(nextTime.frontId()).tell(new RequestNext(), context.self());
        }
      }
    }
  }

  private static class InFlightTime {
    private final GlobalTime globalTime;
    private final boolean accepted;

    private InFlightTime(GlobalTime globalTime, boolean accepted) {
      this.globalTime = globalTime;
      this.accepted = accepted;
    }
  }
}
