package com.spbsu.flamestream.runtime.front;

import akka.actor.Deploy;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.configuration.CommonSerializer;
import com.spbsu.flamestream.runtime.configuration.FrontSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public final class FrontConcierge extends LoggingActor {
  private final FrontSerializer serializer = new CommonSerializer();
  private final ZooKeeper zk;

  private final String prefix;

  private FrontConcierge(String id, ZooKeeper zk) {
    this.prefix = "/workers/" + id + "/fronts";
    this.zk = zk;
  }

  public static Props props(String id, ZooKeeper zk) {
    return Props.create(FrontConcierge.class, id, zk);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    fetchFront();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(WatchedEvent.class, this::onEvent)
            .matchAny(this::unhandled)
            .build();
  }

  private void onEvent(WatchedEvent event) throws KeeperException, InterruptedException {
    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      fetchFront();
    } else {
      log().info("Unexpected event {}", event);
    }
  }

  private void fetchFront() throws KeeperException, InterruptedException {
    final List<String> fronts = zk.getChildren(prefix, selfWatcher());
    for (String frontId : fronts) {
      if (!getContext().findChild(frontId).isPresent()) {
        final byte[] data = zk.getData(prefix + '/' + frontId, false, null);
        final Props frontProps = serializer.deserializeFront(data);
        final Props props = frontProps.withDeploy(Deploy.local());
        getContext().actorOf(props, frontId);
      }
    }
  }

  private Watcher selfWatcher() {
    return event -> self().tell(event, self());
  }
}
