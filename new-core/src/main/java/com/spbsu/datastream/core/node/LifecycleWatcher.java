package com.spbsu.datastream.core.node;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.zookeeper.WatchedEvent;

import static org.apache.zookeeper.Watcher.Event;

public class LifecycleWatcher extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  public static Props props() {
    return Props.create(LifecycleWatcher.class);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    LOG.debug("Got message: {}", message);
    // TODO: 3/10/17 resubscribe?

    if (message instanceof WatchedEvent) {
      final WatchedEvent event = (WatchedEvent) message;
      if (event.getType() == Event.EventType.None) {
        switch (event.getState()) {
          case Expired:
            System.err.print(event);
            System.err.flush();
            System.exit(1);
            break;
        }
      }
    }
  }
}
