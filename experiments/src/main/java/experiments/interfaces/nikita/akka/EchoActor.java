package experiments.interfaces.nikita.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnSuccess;
import akka.japi.Creator;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by marnikitta on 1/31/17.
 */
public class EchoActor extends UntypedActor {
  private final int id;

  private final ActorRef storyTeller;

  private final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));

  private EchoActor(int id, ActorRef storyTeller) {
    this.id = id;
    this.storyTeller = storyTeller;
  }

  public static Props props(int id, ActorRef storyTeller) {
    return Props.create(EchoActor.class, (Creator<EchoActor>) () -> new EchoActor(id, storyTeller));
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    Future<Object> future = Patterns.ask(storyTeller, message, timeout);
    future.onSuccess(new OnSuccess<Object>() {

      @Override
      public void onSuccess(Object result) throws Throwable {
        System.out.println(id + ": " + result);
      }
    }, getContext().dispatcher());
  }
}
