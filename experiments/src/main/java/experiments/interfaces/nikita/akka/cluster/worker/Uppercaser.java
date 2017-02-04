package experiments.interfaces.nikita.akka.cluster.worker;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by marnikitta on 2/2/17.
 */
public class Uppercaser extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), getSelf());

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof String) {
      getSender().tell(((String) message).toUpperCase(), getSelf());
    } else {
      unhandled(message);
    }
  }
}
