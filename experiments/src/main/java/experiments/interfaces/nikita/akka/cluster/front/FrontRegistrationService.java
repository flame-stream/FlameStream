package experiments.interfaces.nikita.akka.cluster.front;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by marnikitta on 2/2/17.
 */
public class FrontRegistrationService extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), getSelf());

  private final Cluster cluster = Cluster.get(context().system());

  public FrontRegistrationService() {
    ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
    mediator.tell(new DistributedPubSubMediator.Subscribe("new-job", getSelf()), getSelf());
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof Integer) {
      LOG.info("Resived :{}", message);
    } else if (message instanceof DistributedPubSubMediator.SubscribeAck) {
      LOG.info("Subscribed");
    } else {
      unhandled(message);
    }
  }
}
