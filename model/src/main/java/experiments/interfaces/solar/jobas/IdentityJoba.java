package experiments.interfaces.solar.jobas;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class IdentityJoba implements Joba {
  private final DataType generates;

  public IdentityJoba(DataType generates) {
    this.generates = generates;
  }

  @Override
  public DataType generates() {
    return generates;
  }

  @Override
  public int id() {
    return -1;
  }

  @Override
  public ActorRef materialize(ActorSystem at, ActorRef sink) {
    return sink;
  }
}
