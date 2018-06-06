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
import java.util.Map;
import java.util.function.Consumer;

public class SourceJoba implements Joba {
  private final Collection<GlobalTime> inFlight = new ArrayList<>();
  private final Map<EdgeId, ActorRef> fronts = new HashMap<>();
  private final int maxInFlightItems;
  private final ActorContext context;

  private int unutilizedRequests;

  public SourceJoba(int maxInFlightItems, ActorContext context) {
    this.maxInFlightItems = maxInFlightItems;
    this.context = context;
    this.unutilizedRequests = maxInFlightItems;
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    sink.accept(item);
    unutilizedRequests--;
    final GlobalTime globalTime = item.meta().globalTime();
    inFlight.add(globalTime);
    requestNext();
  }

  private void requestNext() {
    while (inFlight.size() + unutilizedRequests < maxInFlightItems) {
      unutilizedRequests++;
      fronts.values().forEach(f -> f.tell(new RequestNext(), context.self()));
    }
  }

  public void addFront(EdgeId front, ActorRef actorRef) {
    fronts.putIfAbsent(front, actorRef);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    inFlight.removeIf(nextTime -> nextTime.compareTo(minTime) < 0);
    requestNext();
  }
}
