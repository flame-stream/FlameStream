package experiments.interfaces.solar;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 03.11.16.
 */
public interface Joba {
  DataType generates();
  int id();
  ActorRef materialize(ActorSystem at, ActorRef sink);

  abstract class Stub implements Joba {
    private final DataType generates;
    private final Joba base;
    private ActorRef actor;
    private final int id;

    protected Stub(DataType generates, Joba base) {
      this.generates = generates;
      this.base = base;
      this.id = Output.instance().registerJoba(this);
    }

    protected abstract ActorRef actor(ActorSystem at, ActorRef sink);

    public ActorRef materialize(ActorSystem at, ActorRef sink) {
      if (actor == null)
        actor = actor(at, sink);
      return base.materialize(at, actor);
    }

    @Override
    public DataType generates() {
      return generates;
    }

    @Override
    public int id() {
      return id;
    }
  }
}
