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
import com.spbsu.flamestream.runtime.negitioator.api.AttachSource;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Negotiator extends LoggingActor {
  private final Map<String, ActorRef> localFronts = new HashMap<>();
  private final ActorRef acker;

  @Nullable
  private ActorRef source = null;

  public Negotiator(ActorRef acker) {
    this.acker = acker;
  }

  public static Props props(ActorRef acker) {
    return Props.create(Negotiator.class, acker);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AttachSource.class, mgr -> {
              this.source = mgr.source();
              unstashAll();
            })
            .match(AttachFront.class, attachFront -> {
              if (source != null) {
                localFronts.put(attachFront.frontId(), attachFront.front());
                asyncAttach(attachFront.frontId());
              } else {
                stash();
              }
            })
            .build();
  }

  private void asyncAttach(String frontId) {
    PatternsCS.ask(acker, new RegisterFront(frontId), Timeout.apply(10, SECONDS))
            .thenApply(ticket -> (FrontTicket) ticket)
            .thenAccept(o -> {
              final NewHole newHole = new NewHole(source, o.allowedTimestamp());
              localFronts.get(frontId).tell(newHole, self());
            });
  }
}
