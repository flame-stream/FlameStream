package experiments.nikita.akka.avoidingask;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import experiments.nikita.akka.avoidingask.message.Get;
import experiments.nikita.akka.avoidingask.message.Set;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Created by marnikitta on 2/3/17.
 */
public class RepositoryActor extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private final Map<Integer, String> storage = new HashMap<>();

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof Get) {
      LOG.info(message.toString());
      final int id = ((Get) message).id();

      TimeUnit.SECONDS.sleep(10);
      getSender().tell(Optional.ofNullable(storage.get(id)), getSelf());
    } else if (message instanceof Set) {
      LOG.info(message.toString());
      final int id = ((Set) message).id();
      final String value = ((Set) message).value();

      storage.put(id, value);
    }
  }
}
