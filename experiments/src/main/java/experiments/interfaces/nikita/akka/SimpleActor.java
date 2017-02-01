package experiments.interfaces.nikita.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.routing.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by marnikitta on 1/31/17.
 */
public class SimpleActor extends UntypedActor {
  public static class Message {
    private final String payload;

    public Message(String payload) {
      this.payload = payload;
    }

    public String payload() {
      return payload;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Message{");
      sb.append("payload='").append(payload).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  private ActorRef storyTeller;

  private Router router;

  private SimpleActor(ActorRef storyTeller) {
    this.storyTeller = storyTeller;

  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    List<Routee> routees = new ArrayList<>();

    for (int i = 0; i < 10; ++i) {
      final ActorRef actor = getContext().actorOf(EchoActor.props(i, storyTeller));
      getContext().watch(actor);
      routees.add(new ActorRefRoutee(actor));
    }

    router = new Router(new RoundRobinRoutingLogic(), routees);
  }

  public static Props props(ActorRef storyTeller) {
    return Props.create(SimpleActor.class, () -> new SimpleActor(storyTeller));
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    if (message instanceof Message) {
      router.route(((Message) message).payload() + " SimpleActorWasHere", null);
    } else if (message instanceof Terminated) {
      router.removeRoutee(getSender());
      unhandled(message);
    }
  }
}
