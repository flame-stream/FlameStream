package experiments.interfaces.nikita.akka.avoidingask;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Creator;
import experiments.interfaces.nikita.akka.avoidingask.message.GetRandom;
import experiments.interfaces.nikita.akka.avoidingask.message.Timeout;

import java.util.Optional;

/**
 * Created by marnikitta on 2/3/17.
 */
public class DumpUser extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());
  private final ActorRef service;

  private DumpUser(final ActorRef service) {
    this.service = service;
  }

  public static Props props(final ActorRef service) {
    return Props.create(DumpUser.class, new Creator<DumpUser>() {
      @Override
      public DumpUser create() throws Exception {
        return new DumpUser(service);
      }
    });
  }


  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message.equals("tick")) {
      service.tell(new GetRandom(), getSelf());
    } else if (message instanceof Optional) {
      LOG.info(message.toString());
    } else if (message instanceof Timeout) {
      LOG.error("Timeout :(");
    } else {
      unhandled(message);
    }
  }
}
