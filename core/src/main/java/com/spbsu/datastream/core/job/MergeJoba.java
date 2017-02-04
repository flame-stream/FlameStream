package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.control.Control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MergeJoba extends Joba.AbstractJoba {
  private final List<Joba> jobas = new ArrayList<>();

  //
  public MergeJoba(DataType generates, Joba... jobas) {
    this(generates, Arrays.asList(jobas));
  }

  //
  public MergeJoba(DataType generates, Collection<Joba> jobas) {
    super(generates);
    this.jobas.addAll(jobas);
  }

  //  public void add(Joba oneMore) {
//    jobas.add(oneMore);
//  }
//
//  private ActorRef sink;
//  private ActorRef actor;
//
//  @Override
  public void accept(DataItem item) {

  }

  //
//  @Override
  public void accept(Control control) {

//      return null;
//    if (actor != null)
//      return actor;
//    actor = at.actorOf(ActorContainer.props(MergeActor.class, sink, jobas.size()));
//    this.sink = sink;
//    ActorRef source = null;
//    for (final Joba joba : jobas) {
//      final ActorRef current = joba.materialize(at, actor);
//      if (source == null) {
//        source = current;
//      }
//      else if (source != current && current != null) {
//        throw new RuntimeException("The stream has multiple inputs");
//      }
//    }
//    return source;
//  }
//
//  @SuppressWarnings({"unused", "WeakerAccess"})
//  public static class MergeActor extends ActorAdapter<UntypedActor> {
//    private final ActorRef sink;
//    private final int incoming;
//
//    public MergeActor(ActorRef sink, int incoming) {
//      this.sink = sink;
//      this.incoming = incoming;
//    }
//
//    private int count = 0;
//    private LastItemMarker marker;
//    private boolean itemsReceived = false;
//
//    @ActorMethod
//    public void kill(EndOfTick eot) {
//      count++;
//      if (count == incoming) {
//        sink.tell(eot, self());
//        context().stop(self());
//      }
//      else if (marker == null){
//        itemsReceived = false;
//        marker = new LastItemMarker();
//        sink.tell(marker, self());
//      }
//    }
//
//    @ActorMethod
//    public void marker(LastItemMarker marker) {
//      if (marker == this.marker) {
//        if (itemsReceived) {
//          itemsReceived = false;
//          this.marker = new LastItemMarker();
//          sink.tell(this.marker, self());
//        }
//        else self().tell(new EndOfTick(), self());
//      }
//    }
//
//    @ActorMethod
//    public void merge(DataItem di) {
//      itemsReceived = true;
//      sink.tell(di, self());
//    }
  }
}
