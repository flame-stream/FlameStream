package com.spbsu.flamestream.runtime.edge.negitioator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.master.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.edge.negitioator.api.NewFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Negotiator extends LoggingActor {
  private final ActorRef acker;
  private final ActorRef source;
  private final Map<EdgeId, ActorRef> localFronts = new HashMap<>();

  public Negotiator(ActorRef acker, ActorRef source) {
    this.acker = acker;
    this.source = source;
  }

  public static Props props(ActorRef acker, ActorRef source) {
    return Props.create(Negotiator.class, acker, source);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(NewFront.class, newFront -> {
              localFronts.put(newFront.frontId(), newFront.front());
              log().info("Requesting ticket for the frontClass {}", newFront.frontId());
              asyncAttach(newFront.frontId());
            })
            .build();
  }

  private void asyncAttach(EdgeId edgeId) {
    PatternsCS.ask(acker, new RegisterFront(edgeId), Timeout.apply(10, SECONDS))
            .thenApply(ticket -> (FrontTicket) ticket)
            .thenAccept(ticket -> {
              log().info("Ticket for the frontClass received: {}", ticket);
              localFronts.get(edgeId).tell(new Start(source, ticket.allowedTimestamp()), self());
              localFronts.get(edgeId).tell(new RequestNext(), self());
            });
  }
}
