package experiments.nikita.akka.avoidingask;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import experiments.nikita.akka.avoidingask.message.SetRandom;

/**
 * Created by marnikitta on 2/3/17.
 */
public class Runalka {
  public static void main(final String... args) throws Exception {
    final ActorSystem system = ActorSystem.create();
    final ActorRef repo = system.actorOf(Props.create(RepositoryActor.class), "repository");
    final ActorRef service = system.actorOf(ServiceActor.props(repo));

    for (int i = 0; i < 1000; ++i) {
      service.tell(new SetRandom(), null);
    }

    final ActorRef dumpUser = system.actorOf(DumpUser.props(service));
    dumpUser.tell("tick", null);
  }
}
