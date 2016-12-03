package com.spbsu.datastream.core;


import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.core.job.control.LastItemMarker;

public class MergeActor extends ActorAdapter<UntypedActor>{
  private final Sink sink;
  private final int incoming;

  public MergeActor(Sink sink, int incoming) {
    this.sink = sink;
    this.incoming = incoming;
  }

  private int count = 0;
  private LastItemMarker marker;
  private boolean itemsReceived = false;

  @ActorMethod
  public void kill(EndOfTick eot) {
    count++;
    if (count == incoming) {
      sink.accept(eot);
      context().stop(self());
    }
    else if (marker == null){
      itemsReceived = false;
      marker = new LastItemMarker();
      sink.accept(marker);
    }
  }

  @ActorMethod
  public void marker(LastItemMarker marker) {
    if (marker == this.marker) {
      if (itemsReceived) {
        itemsReceived = false;
        this.marker = new LastItemMarker();
        sink.accept(this.marker);
      }
      else self().tell(new EndOfTick(), self());
    }
  }

  @ActorMethod
  public void merge(DataItem item) {
    itemsReceived = true;
    sink.accept(item);
  }
}