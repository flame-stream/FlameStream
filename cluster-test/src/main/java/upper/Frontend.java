package upper;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by marnikitta on 2/1/17.
 */
public class Frontend extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), getSelf());

  private ActorRef backend;

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message.equals("NOTIFY")) {
      LOG.info("Backend notification");
      backend = getSender();
      getContext().system().scheduler().schedule(Duration.create(1, TimeUnit.SECONDS),
              Duration.create(1, TimeUnit.SECONDS),
              getSelf(), "tick", getContext().dispatcher(), getSelf());
    } else if (message.equals("tick")) {
      backend.tell("abacaba", getSelf());
    } else if (message instanceof String) {
      System.out.println(message);
    }
  }
}
