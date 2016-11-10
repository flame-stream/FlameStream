package experiments.interfaces.solar.jobas;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.control.EndOfTick;
import experiments.interfaces.solar.control.LastItemMarker;

import java.util.*;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MergeJoba extends Joba.Stub {
  private final List<Joba> merge = new ArrayList<>();
  public MergeJoba(DataType generates, Joba... merge) {
    super(generates, null);
    this.merge.addAll(Arrays.asList(merge));
  }

  public void add(Joba oneMore) {
    merge.add(oneMore);
  }

  private ActorRef sink;
  private ActorRef actor;

  @Override
  protected ActorRef actor(ActorSystem at, ActorRef sink) {
    return at.actorOf(ActorContainer.props(MergeActor.class, sink));
  }

  @Override
  public ActorRef materialize(ActorSystem at, ActorRef sink) {
    if (this.sink == sink)
      return null;
    if (actor != null)
      return actor;
    actor = at.actorOf(ActorContainer.props(MergeActor.class, sink, merge.size()));
    this.sink = sink;
    ActorRef source = null;
    for (final Joba joba : merge) {
      final ActorRef current = joba.materialize(at, actor);
      if (source == null) {
        source = current;
      }
      else if (source != current && current != null) {
        throw new RuntimeException("The stream has multiple inputs");
      }
    }
    return source;
  }

  @SuppressWarnings({"unused", "WeakerAccess"})
  public static class MergeActor extends ActorAdapter<UntypedActor> {
    private final ActorRef sink;
    private final int incoming;

    public MergeActor(ActorRef sink, int incoming) {
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
        sink.tell(eot, self());
        context().stop(self());
      }
      else if (marker == null){
        itemsReceived = false;
        marker = new LastItemMarker();
        sink.tell(marker, self());
      }
    }

    @ActorMethod
    public void marker(LastItemMarker marker) {
      if (marker == this.marker) {
        if (itemsReceived) {
          itemsReceived = false;
          this.marker = new LastItemMarker();
          sink.tell(this.marker, self());
        }
        else self().tell(new EndOfTick(), self());
      }
    }

    @ActorMethod
    public void merge(DataItem di) {
      itemsReceived = true;
      sink.tell(di, self());
    }
  }
}
