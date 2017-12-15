package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class SourceJoba extends Joba.Stub {
  /*private final Collection<InFlightTime> inFlight = new ArrayList<>();
  private final Map<EdgeId, ActorRef> fronts = new HashMap<>();
  private final int maxInFlightItems;*/

  public SourceJoba(int maxInFlightItems, Stream<Joba> outJobas, ActorRef acker, ActorContext context) {
    super(outJobas, acker, context);
    //this.maxInFlightItems = maxInFlightItems;
  }

  public void addFront(EdgeId front, ActorRef actorRef) {
    //fronts.putIfAbsent(front, actorRef);
  }

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
  public void accept(DataItem dataItem, boolean fromAsync) {
    process(dataItem, Stream.of(dataItem), fromAsync);
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

  @Override
  public boolean isAsync() {
    return false;
  }

  /*private static class InFlightTime {
    private final GlobalTime globalTime;
    private final boolean accepted;

    private InFlightTime(GlobalTime globalTime, boolean accepted) {
      this.globalTime = globalTime;
      this.accepted = accepted;
    }
  }*/
}
