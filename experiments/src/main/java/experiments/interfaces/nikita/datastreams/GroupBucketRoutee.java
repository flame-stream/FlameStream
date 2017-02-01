package experiments.interfaces.nikita.datastreams;

import akka.actor.ActorRef;
import akka.routing.Routee;

/**
 * Created by marnikitta on 2/1/17.
 */
public class GroupBucketRoutee implements Routee {
  private final int hash;

  private final ActorRef bucketActor;

  public GroupBucketRoutee(int hash, ActorRef bucketActor) {
    this.hash = hash;
    this.bucketActor = bucketActor;
  }

  @Override
  public void send(Object message, ActorRef sender) {
    bucketActor.tell(message, sender);
  }

  public int hash() {
    return hash;
  }
}
