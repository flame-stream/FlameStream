package experiments.interfaces.nikita.akka.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by marnikitta on 2/2/17.
 */
public class MasterPublisher extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), getSelf());

  private final Cluster cluster = Cluster.get(context().system());
  private final ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
  private final ThreadLocalRandom rd = ThreadLocalRandom.current();


  public void onReceive(final Object message) throws Throwable {
    if (message.equals("tick")) {
      mediator.tell(new DistributedPubSubMediator.Publish("new-job", rd.nextInt()), getSelf());
    } else {
      unhandled(message);
    }
  }
}
