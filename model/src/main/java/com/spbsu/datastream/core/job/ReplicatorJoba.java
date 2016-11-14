package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.*;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ReplicatorJoba extends Joba.Stub {
  public ReplicatorJoba(final Joba base) {
    super(base.generates(), base);
  }

  private List<ActorRef> sinks = new ArrayList<>();
  @Override
  public ActorRef materialize(ActorSystem at, ActorRef sink) {
    sinks.add(sink);
    return super.materialize(at, sink);
  }

  @Override
  protected ActorRef actor(ActorSystem at, ActorRef sink) {
    return at.actorOf(ActorContainer.props(ReplicatorActor.class, this));
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class ReplicatorActor extends ActorAdapter<UntypedActor> {
    private final ReplicatorJoba padre;
    public ReplicatorActor(ReplicatorJoba padre) {
      this.padre = padre;
    }

    @ActorMethod
    public void control(Control control) {
      for (int i = padre.sinks.size() - 1; i >= 0; i--) {
        final ActorRef next = padre.sinks.get(i);
        if (control instanceof EndOfTick && next.isTerminated())
          continue;
        next.tell(control, sender());
      }
      if (control instanceof EndOfTick)
        context().stop(self());
    }

    @ActorMethod
    public void broadcast(DataItem di) {
      for (ActorRef sink: padre.sinks) {
        sink.tell(di, self());
      }
    }
  }
}
