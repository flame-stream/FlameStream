package experiments.interfaces.nikita.akka.cluster.worker;

import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by marnikitta on 2/2/17.
 */
public class Uppercaser extends UntypedActor {
  private final Cluster cluster = Cluster.get(getContext().system());
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), getSelf());

  @Override
  public void preStart() throws Exception {
    System.out.println("AMA UP");
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof String) {
      final String msg = (String) message;
      LOG.info("Id: {}, Message: {}", cluster.selfAddress(), msg.toUpperCase());
    } else {
      unhandled(message);
    }
  }
}
