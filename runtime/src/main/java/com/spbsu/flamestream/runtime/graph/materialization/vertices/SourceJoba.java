package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.EdgeInstance;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class SourceJoba implements VertexJoba {
  private final Collection<InFlightTime> inFlight = new ArrayList<>();
  private final Map<EdgeInstance, ActorRef> fronts = new HashMap<>();

  private final int maxInFlightItems;
  private final ActorContext context;
  private final Consumer<GlobalTime> heartBeater;
  private final Consumer<DataItem> sink;

  public SourceJoba(int maxInFlightItems, ActorContext context, Consumer<GlobalTime> heartBeater, Consumer<DataItem> sink) {
    this.maxInFlightItems = maxInFlightItems;
    this.context = context;
    this.heartBeater = heartBeater;
    this.sink = sink;
  }

  public void addFront(EdgeInstance front, ActorRef actorRef) {
    fronts.putIfAbsent(front, actorRef);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    /*final Iterator<InFlightTime> iterator = inFlight.iterator();
    while (iterator.hasNext()) {
      final InFlightTime next = iterator.next();
      final GlobalTime nextTime = next.globalTime;
      if (nextTime.compareTo(minTime) <= 0) {
        iterator.remove();
        if (!next.accepted) {
          fronts.get(nextTime.frontId()).tell(new RequestNext(nextTime), context.self());
        }
      }
    }*/
  }

  @Override
  public void accept(DataItem dataItem) {
    sink.accept(dataItem);
    /*{ //back-pressure logic
      final GlobalTime globalTime = dataItem.meta().globalTime();
      if (inFlight.size() < maxInFlightItems) {
        fronts.get(globalTime.frontId()).tell(new RequestNext(globalTime), context.self());
        inFlight.add(new InFlightTime(globalTime, true));
      } else {
        heartBeater.accept(globalTime);
        inFlight.add(new InFlightTime(globalTime, false));
      }
    }*/
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
