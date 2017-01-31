package experiments.interfaces.nikita.akka;

import akka.actor.UntypedActor;

/**
 * Created by marnikitta on 1/31/17.
 */
public class EchoActor extends UntypedActor {
  @Override
  public void onReceive(Object message) throws Throwable {
    System.out.println(message);
  }
}
