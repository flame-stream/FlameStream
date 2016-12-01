package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.*;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ReplicatorJoba extends Joba.Stub {
  public ReplicatorJoba(Sink... lists) {
    this(Arrays.asList(lists));
  }

  public ReplicatorJoba(Collection<Sink> sinks) {
    super(null);
    this.sinks.addAll(sinks);
  }

  public void add(Sink sink){
    sinks.add(sink);
  }

  private List<Sink> sinks = new ArrayList<>();

  @Override
  public void accept(DataItem item) {
    for (Sink sink: sinks) {
      sink.accept(item);
    }
  }

  @Override
  public void accept(Control control) {
    for (Sink sink: sinks) {
      sink.accept(control);
    }
  }
}
