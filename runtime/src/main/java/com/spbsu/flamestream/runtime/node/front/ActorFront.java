package com.spbsu.flamestream.runtime.node.front;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.Accepted;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.NewHole;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.PleaseWait;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.Replay;
import com.spbsu.flamestream.runtime.environment.raw.RawData;
import org.jetbrains.annotations.Nullable;
import scala.Option;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public class ActorFront<T> extends LoggingActor {
  private final String frontId;
  private final ActorPath remoteActor;

  private long prevGlobalTs = 0;
  private ActorRef hole = context().system().deadLetters();

  private final Queue<T> queue = new ArrayDeque<>();
  private final NavigableSet<DataItem<T>> history = new ConcurrentSkipListSet<>(Comparator.comparing(DataItem::meta));

  @Nullable
  private DataItem<T> pending = null;
  @Nullable
  private Cancellable ping;

  private ActorFront(String frontId, ActorPath remoteActor) {
    this.frontId = frontId;
    this.remoteActor = remoteActor;
  }

  public static Props props(String frontId, ActorPath path) {
    return Props.create(ActorFront.class, frontId, path);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    context().actorSelection(remoteActor).tell(new ActorIdentity("hi", Option.apply(self())), self());

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
            .match(NewHole.class, this::onNewHole)
            .match(Replay.class, this::onReplay)
            .match(Accepted.class, this::onAccepted)
            .match(PleaseWait.class, this::onPleaseWait)
            .match(String.class, s -> s.equals("ping"), p -> emmitWatermark())
            .build();
  }

  private void onRaw(RawData<T> data) {
    data.forEach(queue::add);
    if (pending == null) {
      emmit();
    }
  }

  private void onPleaseWait(PleaseWait pleaseWait) {
    context().system().scheduler().scheduleOnce(
            FiniteDuration.apply(pleaseWait.durationMillis(), TimeUnit.MILLISECONDS),
            this::emmit,
            context().system().dispatcher()
    );
  }

  private void onAccepted(Accepted accepted) {
    //noinspection ConstantConditions
    if (accepted.globalTime().equals(pending.meta().globalTime())) {
      pending = null;
      emmit();
    } else {
      throw new IllegalStateException("Unexpected ack"); // TODO: 13.11.2017 think about me
    }
  }

  private void onNewHole(NewHole h) {
    hole = h.hole();
    emmit();
  }

  private void onReplay(Replay replay) {
    history.subSet(
            new PayloadDataItem<>(Meta.meta(replay.from()), null),
            new PayloadDataItem<>(Meta.meta(replay.to()), null)
    ).forEach(dataItem -> hole.tell(dataItem, sender()));
  }

  private void emmitWatermark() {
    hole.tell(new Heartbeat(currentMeta().globalTime()), self());
  }

  private void emmit() {
    if (pending == null && !queue.isEmpty()) {
      final T element = queue.poll();
      pending = new PayloadDataItem<>(currentMeta(), element);
      history.add(pending);
      hole.tell(pending, self());
    } else if (pending != null) {
      hole.tell(pending, null);
    }
  }

  private Meta currentMeta() {
    long globalTs = System.currentTimeMillis();
    if (globalTs <= prevGlobalTs) {
      globalTs = prevGlobalTs + 1;
    }
    prevGlobalTs = globalTs;

    final GlobalTime globalTime = new GlobalTime(globalTs, frontId);
    return Meta.meta(globalTime);
  }
}
