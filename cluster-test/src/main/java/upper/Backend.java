package upper;

import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.dispatch.Futures;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import scala.concurrent.Future;

/**
 * Created by marnikitta on 2/1/17.
 */
public class Backend extends UntypedActor {
  private final Cluster cluster = Cluster.get(getContext().system());
  private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), getSelf());

  @Override
  public void preStart() throws Exception {
    cluster.subscribe(getSelf(), ClusterEvent.MemberUp.class);
  }

  @Override
  public void postStop() throws Exception {
    cluster.unsubscribe(getSelf());
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof ClusterEvent.MemberUp) {
      final ClusterEvent.MemberUp memberUp = (ClusterEvent.MemberUp) message;
      register(memberUp.member());
    } else if (message instanceof String) {
      final String string = (String) message;
      Future<String> future = Futures.future(() -> upper(string), getContext().dispatcher());
      Patterns.pipe(future, getContext().dispatcher()).to(getSender());
    }
  }

  private void register(Member member) {
    getContext().actorSelection(member.address() + "/user/frontend").tell("NOTIFY", getSelf());
    LOG.info("Noti" + member.address());
  }

  private String upper(final String payload) {
    return payload.toUpperCase();
  }
}
