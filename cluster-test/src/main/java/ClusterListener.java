import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by marnikitta on 2/1/17.
 */
public class ClusterListener extends UntypedActor {
  private LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);
  private Cluster cluster = Cluster.get(getContext().system());

  @Override
  public void preStart() {
    cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
            ClusterEvent.MemberEvent.class, ClusterEvent.UnreachableMember.class);
  }

  @Override
  public void postStop() {
    cluster.unsubscribe(getSelf());
  }

  @Override
  public void onReceive(Object message) {
    if (message instanceof ClusterEvent.MemberUp) {
      ClusterEvent.MemberUp mUp = (ClusterEvent.MemberUp) message;
      LOG.info("Member is Up: {}", mUp.member());
    } else if (message instanceof ClusterEvent.UnreachableMember) {
      ClusterEvent.UnreachableMember mUnreachable = (ClusterEvent.UnreachableMember) message;
      LOG.info("Member detected as unreachable: {}", mUnreachable.member());
    } else if (message instanceof ClusterEvent.MemberRemoved) {
      ClusterEvent.MemberRemoved mRemoved = (ClusterEvent.MemberRemoved) message;
      LOG.info("Member is Removed: {}", mRemoved.member());
    } else if (message instanceof ClusterEvent.MemberEvent) {
    } else {
      unhandled(message);
    }
  }
}
