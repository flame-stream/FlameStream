package com.spbsu.flamestream.runtime.negitioator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.edge.front.api.OnStart;
import com.spbsu.flamestream.runtime.edge.front.api.RequestNext;
import com.spbsu.flamestream.runtime.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Negotiator extends LoggingActor {
  private final String nodeId;
  private final ActorRef acker;
  private final ActorRef source;
  private final Map<String, ActorRef> localFronts = new HashMap<>();

  public Negotiator(String nodeId, ActorRef acker, ActorRef source) {
    this.nodeId = nodeId;
    this.acker = acker;
    this.source = source;
  }

  public static Props props(String nodeId, ActorRef acker, ActorRef source) {
    return Props.create(Negotiator.class, nodeId, acker, source);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AttachFront.class, attachFront -> {
              localFronts.put(attachFront.frontId(), attachFront.front());
              log().info("Requesting ticket for the frontClass {}", attachFront.frontId());
              asyncAttach(attachFront.frontId());
            })
            .build();
  }

  private void asyncAttach(String frontId) {
    PatternsCS.ask(acker, new RegisterFront(frontId, nodeId), Timeout.apply(10, SECONDS))
            .thenApply(ticket -> (FrontTicket) ticket)
            .thenAccept(ticket -> {
              log().info("Ticket for the frontClass received: {}", ticket);
              localFronts.get(frontId).tell(new OnStart(source), self());
              localFronts.get(frontId).tell(new RequestNext(ticket.allowedTimestamp()), self());
            });
  }
}
