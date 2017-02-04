package experiments.interfaces.nikita.akka.avoidingask;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import experiments.interfaces.nikita.akka.avoidingask.message.*;
import scala.concurrent.duration.Duration;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Created by marnikitta on 2/3/17.
 */
public class ServiceActor extends UntypedActor {
  private final ThreadLocalRandom rd = ThreadLocalRandom.current();

  private final ActorRef repository;

  public static Props props(final ActorRef repository) {
    return Props.create(ServiceActor.class, new Creator<ServiceActor>() {
      @Override
      public ServiceActor create() throws Exception {
        return new ServiceActor(repository);
      }
    });
  }

  private ServiceActor(final ActorRef repository) {
    this.repository = repository;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof SetRandom) {
      final int id = rd.nextInt(10);
      final String value = UUID.randomUUID().toString();
      repository.tell(new Set(id, value), null);
    } else if (message instanceof GetRandom) {
      final int id = rd.nextInt(10);

      final ActorRef inner = context().actorOf(InnerActor.props(sender()));
      repository.tell(new Get(id), inner);
      context().system().scheduler()
              .scheduleOnce(Duration.create(5, TimeUnit.SECONDS), inner, new Timeout(), context().dispatcher(), getSelf());
    }
  }

  private static class InnerActor extends UntypedActor {
    private final ActorRef originalSender;

    private InnerActor(final ActorRef originalSender) {
      this.originalSender = originalSender;
    }

    public static Props props(final ActorRef sender) {
      return Props.create(InnerActor.class, new Creator<InnerActor>() {
        @Override
        public InnerActor create() throws Exception {
          return new InnerActor(sender);
        }
      });
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onReceive(final Object message) throws Throwable {
      if (message instanceof Optional) {
        responseAndSuicide(message);
      } else if (message instanceof Timeout) {
        responseAndSuicide(message);
      } else {
        unhandled(message);
      }
    }

    private void responseAndSuicide(final Object response) {
      originalSender.tell(response, self());
      context().stop(self());
    }
  }
}
