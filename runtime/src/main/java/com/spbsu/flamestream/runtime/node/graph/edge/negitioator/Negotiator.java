package com.spbsu.flamestream.runtime.node.graph.edge.negitioator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.runtime.node.graph.edge.front.api.NewHole;
import com.spbsu.flamestream.runtime.node.graph.edge.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.node.graph.edge.negitioator.api.LocalFront;
import com.spbsu.flamestream.runtime.node.graph.edge.negitioator.api.LocalSourceEntrance;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Negotiator extends LoggingActor {
  private final Map<String, LocalSourceEntrance> localSources = new HashMap<>();
  private final Map<String, LocalFront> localFronts = new HashMap<>();

  private Negotiator() {
  }

  public static Props props() {
    return Props.create(Negotiator.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(LocalSourceEntrance.class, localSourceEntrance -> {
              localSources.put(localSourceEntrance.graphId(), localSourceEntrance);
              unstashAll();
            })
            .match(LocalFront.class, localFront -> {
              localFronts.put(localFront.frontId(), localFront);
              unstashAll();
            })
            .match(AttachFront.class, attachFront -> {
              if (localSources.containsKey(attachFront.graphId())
                      && localFronts.containsKey(attachFront.frontId())) {
                final ActorRef acker = localSources.get(attachFront.graphId()).acker();
                final ActorRef front = localFronts.get(attachFront.graphId()).front();
                final ActorRef source = localSources.get(attachFront.graphId()).source();
                PatternsCS.ask(
                        acker,
                        new RegisterFront(attachFront.frontId()),
                        Timeout.apply(2, SECONDS)
                ).thenAccept(o -> {
                  final NewHole newHole = new NewHole(source, ((FrontTicket) o).allowedTimestamp());
                  front.tell(newHole, acker);
                });
              } else {
                stash();
              }
            })
            .build();
  }
}
