package experiments.interfaces.nikita.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.Random;

/**
 * Created by marnikitta on 1/31/17.
 */
public class Main {
  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create();

    ActorRef echo = system.actorOf(Props.create(EchoActor.class));
    ActorRef simple = system.actorOf(Props.create(SimpleActor.class, echo));

    new Random().ints().limit(100).forEach(i -> simple.tell(i, null));
  }
}
