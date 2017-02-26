package experiments.nikita.akka.cluster.master;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by marnikitta on 2/2/17.
 */
public class MasterActor extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), getSelf());
  private final List<ActorRef> workers = new ArrayList<>();

  private final Cluster cluster = Cluster.get(context().system());

  @Override
  public void preStart() throws Exception {
    cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
            ClusterEvent.MemberEvent.class);
  }

  public void onReceive(final Object message) throws Throwable {
    if (message instanceof ClusterEvent.MemberUp) {
      final ClusterEvent.MemberUp memberUp = (ClusterEvent.MemberUp) message;
      requestActor(memberUp.member());
    } else if (message instanceof ActorIdentity) {
      Optional.ofNullable(((ActorIdentity) message).getRef())
              .ifPresent(workers::add);
    } else if (message.equals("tick")) {
      workers.forEach(w -> w.tell("abacaba", getSelf()));
    } else if (message instanceof String) {
      LOG.info((String) message);
    } else {
      unhandled(message);
    }
  }

  private void requestActor(final Member member) {
    if (member.getRoles().contains("worker")) {
      final Address nodeAddress = member.address();
      ActorSelection selection = getContext().system().actorSelection(nodeAddress.toString() + "/user/uppercaser");
      selection.tell(new Identify(1), getSelf());
    }
  }
}
