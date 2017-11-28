package com.spbsu.flamestream.runtime.node.watcher;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashSet;
import java.util.Set;

public class GraphWatcher extends LoggingActor {
  private final Set<String> seenGraphs = new HashSet<>();
  private final GraphClient client;
  private final ActorRef subscriber;

  private GraphWatcher(GraphClient client, ActorRef subscriber) {
    this.client = client;
    this.subscriber = subscriber;
  }

  public static Props props(GraphClient client, ActorRef subscriber) {
    return Props.create(GraphWatcher.class, client, subscriber);
  }

  @Override
  public void preStart() throws Exception {
    final Set<String> graphs = client.graphs(s -> self().tell(s, ActorRef.noSender()));
    self().tell(graphs, ActorRef.noSender());
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Set.class, graphIds -> {
              final Set<String> newIds = new HashSet<>(graphIds);
              newIds.removeAll(seenGraphs);
              newIds.stream().map(client::graphById).forEach(graph -> subscriber.tell(graph, self()));
              seenGraphs.addAll(newIds);
            })
            .build();
  }
}
