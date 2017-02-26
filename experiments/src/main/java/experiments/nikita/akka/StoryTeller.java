package experiments.nikita.akka;

import akka.actor.UntypedActor;

/**
 * Created by marnikitta on 1/31/17.
 */
public class StoryTeller extends UntypedActor {

  @Override
  public void onReceive(Object message) throws Throwable {
    sender().tell(message + " StoryTellerWasHere", self());
  }
}
