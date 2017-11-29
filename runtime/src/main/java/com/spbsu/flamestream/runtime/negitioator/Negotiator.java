package com.spbsu.flamestream.runtime.negitioator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.edge.front.api.NewHole;
import com.spbsu.flamestream.runtime.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Negotiator extends LoggingActor {
  private final Map<String, ActorRef> localFronts = new HashMap<>();
  private final ActorRef acker;
  private final ActorRef source;

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
            .match(AttachFront.class, attachFront -> {
              localFronts.put(attachFront.frontId(), attachFront.front());
              log().info("Requesting ticket for the front {}", attachFront.frontId());
              asyncAttach(attachFront.frontId());
            })
            .build();
  }

  private void asyncAttach(String frontId) {
    PatternsCS.ask(acker, new RegisterFront(frontId), Timeout.apply(10, SECONDS))
            .thenApply(ticket -> (FrontTicket) ticket)
            .thenAccept(ticket -> {
              log().info("Ticket for the front received: {}", ticket);
              final NewHole newHole = new NewHole(source, ticket.allowedTimestamp());
              localFronts.get(frontId).tell(newHole, self());
            });
  }
}
