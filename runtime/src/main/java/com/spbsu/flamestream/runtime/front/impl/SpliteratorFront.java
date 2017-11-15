package com.spbsu.flamestream.runtime.front.impl;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.graph.source.api.*;
import org.jetbrains.annotations.Nullable;
import scala.concurrent.duration.FiniteDuration;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public final class SpliteratorFront<T> extends LoggingActor {
  private final NavigableSet<DataItem<T>> history = new ConcurrentSkipListSet<>(Comparator.comparing(DataItem::meta));
  private final String frontId;
  private final Spliterator<T> spliterator;

  private long prevGlobalTs = 0;
  private ActorRef hole = context().system().deadLetters();

  @Nullable
  private DataItem<T> pending = null;

  private SpliteratorFront(String frontId, Spliterator<T> spliterator) {
    this.frontId = frontId;
    this.spliterator = spliterator;
  }

  public static <T> Props props(String frontId, Spliterator<T> spliterator) {
    return Props.create(SpliteratorFront.class, frontId, spliterator);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(NewHole.class, this::onNewHole)
            .match(Replay.class, this::onReplay)
            .match(Accepted.class, this::onAccepted)
            .match(PleaseWait.class, this::onPleaseWait)
            .build();
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
      final boolean advanced = spliterator.tryAdvance(element -> {
        pending = new PayloadDataItem<>(currentMeta(), element);
        history.add(pending);
        hole.tell(pending, self());
      });
      if (!advanced) {
        hole.tell(new Heartbeat(new GlobalTime(Long.MAX_VALUE, frontId)), self());
      }
    } else {
      hole.tell(pending, null);
    }
  }

  private GlobalTime currentGlobalTime() {
    long globalTs = System.currentTimeMillis();
    if (globalTs <= prevGlobalTs) {
      globalTs = prevGlobalTs + 1;
    }
    prevGlobalTs = globalTs;
    return new GlobalTime(globalTs, frontId);
  }

  private Meta currentMeta() {
    return Meta.meta(currentGlobalTime());
  }
}
