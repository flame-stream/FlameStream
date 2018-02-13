package com.spbsu.flamestream.runtime.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LocalAcker extends LoggingActor {
  private final Map<GlobalTime, Long> ackCache = new HashMap<>();
  private final List<Heartbeat> heartbeatCache = new ArrayList<>();

  private final ActorRef globalAcker;
  private final ActorRef pingActor;

  public LocalAcker(ActorRef globalAcker) {
    this.globalAcker = globalAcker;
    pingActor = context().actorOf(PingActor.props(self(), Flush.FLUSH).withDispatcher("resolver-dispatcher"));
  }

  public static Props props(ActorRef globalAcker) {
    return Props.create(LocalAcker.class, globalAcker);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(10)), self());
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, heartbeatCache::add)
            .match(RegisterFront.class, registerFront -> globalAcker.forward(registerFront, context()))
            .match(UnregisterFront.class, unregisterFront -> globalAcker.forward(unregisterFront, context()))
            .match(Flush.class, flush -> flush())
            .build();
  }

  private void handleAck(Ack ack) {
    ackCache.compute(ack.time(), (globalTime, xor) -> {
      if (xor == null) {
        return ack.xor();
      } else {
        return ack.xor() ^ xor;
      }
    });
  }

  private void flush() {
    ackCache.forEach((globalTime, xor) -> globalAcker.tell(new Ack(globalTime, xor), context().parent()));
    ackCache.clear();

    heartbeatCache.forEach(heartbeat -> globalAcker.tell(heartbeat, self()));
    heartbeatCache.clear();
  }

  private enum Flush {
    FLUSH
  }
}
