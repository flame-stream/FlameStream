package experiments.interfaces.nikita.akka;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

/**
 * Created by marnikitta on 1/31/17.
 */
public class SimpleActor extends UntypedActor {
  private final ActorRef sink;

  public SimpleActor(ActorRef sink) {
    this.sink = sink;
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    sink.tell(message + " SimpleActorWasHere", null);
  }
}
