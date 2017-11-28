package com.spbsu.flamestream.runtime.negitioator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.runtime.edge.front.api.NewHole;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.negitioator.api.NewMaterialization;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Negotiator extends LoggingActor {
  private final Map<String, ActorRef> localFronts = new HashMap<>();

  @Nullable
  private ActorRef currentSource;

  @Nullable
  private ActorRef currentAcker;

  private Negotiator() {
  }

  public static Props props() {
    return Props.create(Negotiator.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(NewMaterialization.class, mat -> {
              currentAcker = mat.acker();
              currentSource = mat.source();
              localFronts.keySet().forEach(this::asyncAttach);
            })
            .match(AttachFront.class, attachFront -> {
              localFronts.put(attachFront.frontId(), attachFront.front());
              asyncAttach(attachFront.frontId());
            })
            .build();
  }

  private void asyncAttach(String frontId) {
    PatternsCS.ask(currentAcker, new RegisterFront(frontId), Timeout.apply(10, SECONDS))
            .thenApply(ticket -> (FrontTicket) ticket)
            .thenAccept(o -> {
              final NewHole newHole = new NewHole(currentSource, o.allowedTimestamp());
              localFronts.get(frontId).tell(newHole, self());
            });
  }
}
