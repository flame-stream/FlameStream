package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jetbrains.annotations.Nullable;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AkkaFront implements Front {
  private final ActorRef innerActor;

  public AkkaFront(SystemEdgeContext context) {
    this.innerActor = context.refFactory()
            .actorOf(InnerActor.props(context.edgeId()), context.edgeId() + "-inner");
  }

  @Override
  public void onStart(Consumer<Object> consumer) {
    innerActor.tell(consumer, ActorRef.noSender());
  }

  @Override
  public void onRequestNext(GlobalTime from) {
    innerActor.tell(new RequestNext(from), ActorRef.noSender());
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    innerActor.tell(new RequestNext(to), ActorRef.noSender());
  }

  private static class InnerActor extends LoggingActor {
    private final EdgeId frontId;

    @Nullable
    private Cancellable ping;

    @Nullable
    private Consumer<Object> hole = null;

    private final NavigableMap<GlobalTime, Object> history = new TreeMap<>();

    private long prevGlobalTs = 0;

    private InnerActor(EdgeId frontId) {
      this.frontId = frontId;
    }

    public static Props props(EdgeId frontId) {
      return Props.create(InnerActor.class, frontId);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      this.ping = context().system().scheduler().schedule(
              Duration.Zero(),
              FiniteDuration.apply(100, TimeUnit.MILLISECONDS),
              self(),
              "ping",
              context().system().dispatcher(),
              self()
      );
    }

    @Override
    public void postStop() {
      if (ping != null) {
        ping.cancel();
      }
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(RawData.class, this::onRaw)
              .match(Consumer.class, this::onStart)
              .match(RequestNext.class, requestNext -> onRequestNext(requestNext.time()))
              .match(Checkpoint.class, checkpoint -> onCheckpoint(checkpoint.time()))
              .match(String.class, s -> s.equals("ping"), p -> emmitHeartbeat())
              .build();
    }

    private void onStart(Consumer<Object> consumer) {
      this.hole = consumer;
      unstashAll();
    }

    private void onRequestNext(GlobalTime time) {
      unstashAll();
    }

    private void onCheckpoint(GlobalTime to) {
      history.headMap(to).clear();
    }

    private void onRaw(RawData<Object> data) {
      if (hole == null) {
        stash();
      } else {
        final PayloadDataItem t = new PayloadDataItem(new Meta(currentTime()), data.data());
        hole.accept(t);
      }
    }

    private void emmitHeartbeat() {
      if (hole != null) {
        hole.accept(new Heartbeat(currentTime()));
      }
    }

    private GlobalTime currentTime() {
      long globalTs = System.currentTimeMillis();
      if (globalTs <= prevGlobalTs) {
        globalTs = prevGlobalTs + 1;
      }
      prevGlobalTs = globalTs;


      return new GlobalTime(globalTs, frontId);
    }
  }
}