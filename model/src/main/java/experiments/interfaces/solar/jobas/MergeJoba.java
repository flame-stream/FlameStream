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
import experiments.interfaces.solar.items.EndOfTick;

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

    int count = 0;
    @ActorMethod
    public void kill(EndOfTick eot) {
      if (count == 0)
        sink.tell(eot, self());
      if (sender() == self())
        count++;
      if (count == incoming) // all incoming have finished their jobs
        self().tell(PoisonPill.getInstance(), self());
    }

    @ActorMethod
    public void merge(DataItem di) {
      sink.tell(di, self());
      if (count > 0)
        count--;
    }

    @Override
    protected void postStop() {
      sink.tell(PoisonPill.getInstance(), self());
    }
  }
}
