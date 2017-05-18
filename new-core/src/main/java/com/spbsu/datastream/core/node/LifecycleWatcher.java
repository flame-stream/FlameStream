package com.spbsu.datastream.core.node;

import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.LoggingActor;
import org.apache.zookeeper.WatchedEvent;

import static org.apache.zookeeper.Watcher.Event;

public final class LifecycleWatcher extends LoggingActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  public static Props props() {
    return Props.create(LifecycleWatcher.class);
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder().match(WatchedEvent.class, LifecycleWatcher::onWatchedEvent).build();
  }

  private static void onWatchedEvent(final WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();
      if (state == Event.KeeperState.Expired) {
        System.err.print(event);
        System.err.flush();
        // TODO: 3/26/17 DO NOT EXIT HERE
        System.exit(1);
      }
    }
  }
}
