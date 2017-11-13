package com.spbsu.flamestream.runtime.front;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.raw.RawData;
import com.spbsu.flamestream.runtime.source.api.Accepted;
import com.spbsu.flamestream.runtime.source.api.NewHole;
import com.spbsu.flamestream.runtime.source.api.PleaseWait;
import com.spbsu.flamestream.runtime.source.api.Replay;
import org.jetbrains.annotations.Nullable;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public final class ActorFront<T> extends LoggingActor {
  private final int frontId;
  private final ActorPath remoteActor;

  private long prevGlobalTs = 0;
  private ActorRef hole = context().system().deadLetters();

  private final Queue<T> queue = new ArrayDeque<>();

  @Nullable
  private DataItem<T> pending = null;

  private final NavigableSet<DataItem<T>> history = new ConcurrentSkipListSet<>(Comparator.comparing(DataItem::meta));

  private ActorFront(int frontId, ActorPath remoteActor) {
    this.frontId = frontId;
    this.remoteActor = remoteActor;
  }

  public static <T> Props props(int frontId, ActorPath path) {
    return Props.create(ActorFront.class, frontId, path);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    context().actorSelection(remoteActor).tell(new ActorIdentity("hi", Option.apply(self())), self());
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(RawData.class, this::onRaw)
            .match(NewHole.class, this::onNewHole)
            .match(Replay.class, this::onReplay)
            .match(Accepted.class, this::onAccepted)
            .match(PleaseWait.class, this::onPleaseWait)
            .build();
  }

  private void onRaw(RawData<T> data) {
    data.forEach(queue::add);
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
      throw new IllegalStateException("Unexpected ack");
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

  private void emmit() {
    if (pending == null) {
      spliterator.tryAdvance(element -> {
        pending = new PayloadDataItem<>(currentMeta(), element);
        history.add(pending);
        hole.tell(pending, self());
      });
    } else {
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
