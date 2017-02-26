package experiments.nikita.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.Random;

/**
 * Created by marnikitta on 1/31/17.
 */
public class Main {
  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create();

    ActorRef storyTeller = system.actorOf(Props.create(StoryTeller.class));

    system.actorOf(SimpleActor.props(storyTeller), "simple");

    ActorSelection ref = system.actorSelection("akka://default/user/simple");
    new Random().ints().limit(100).mapToObj(Integer::toString).map(SimpleActor.Message::new)
            .forEach(i -> ref.tell(i, null));

  }
}
