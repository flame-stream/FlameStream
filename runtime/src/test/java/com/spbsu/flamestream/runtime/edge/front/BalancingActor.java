package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class BalancingActor extends LoggingActor {
  private final List<ActorRef> fronts = new ArrayList<>();

  public static Props props() {
    return Props.create(BalancingActor.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(ActorRef.class, ref -> {
              fronts.add(sender());
              unstashAll();

              getContext().become(ReceiveBuilder.create()
                      .match(ActorRef.class, r -> fronts.add(sender()))
                      .matchAny(
                              r -> fronts.get(ThreadLocalRandom.current().nextInt(fronts.size()))
                                      .tell(new RemoteActorFront.RawData<>(r), self())
                      )
                      .build()
              );
            })
            .matchAny(o -> stash())
            .build();
  }
}
