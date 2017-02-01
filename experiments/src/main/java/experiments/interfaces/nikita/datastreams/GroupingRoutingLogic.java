package experiments.interfaces.nikita.datastreams;

import akka.routing.Routee;
import akka.routing.RoutingLogic;
import scala.collection.immutable.IndexedSeq;

/**
 * Created by marnikitta on 2/1/17.
 */
public class GroupingRoutingLogic implements RoutingLogic {
  private final int bucketNum;

  public GroupingRoutingLogic(int bucketNum) {
    this.bucketNum = bucketNum;
  }

  @Override
  public Routee select(Object message, IndexedSeq<Routee> routees) {
    //// TODO: 2/1/17
    return null;
  }
}

