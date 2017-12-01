package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.Cancellable;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.edge.front.api.OnStart;
import com.spbsu.flamestream.runtime.edge.front.api.RequestNext;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jetbrains.annotations.Nullable;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RemoteActorFront<T> extends LoggingActor implements Front {
  private final String frontId;
  private final String path;

  @Nullable
  private Cancellable ping;

  private final NavigableMap<GlobalTime, T> history = new TreeMap<>();

  @Nullable
  private Consumer<Object> hole = null;

  @Nullable
  private GlobalTime unsatisfiedRequest = null;
  private long prevGlobalTs = 0;

  private RemoteActorFront(String frontId, String path) {
    this.frontId = frontId;
    this.path = path;
  }

  public static Props props(String frontId, String path) {
    return Props.create(RemoteActorFront.class, frontId, path);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    context().actorSelection(path).tell(self(), self());

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
            .match(OnStart.class, start -> onStart(di -> start.consumer().tell(di, sender())))
            .match(RequestNext.class, requestNext -> onRequestNext(requestNext.time()))
            .match(String.class, s -> s.equals("ping"), p -> emmitHeartbeat())
            .build();
  }

  @Override
  public void onStart(Consumer<Object> consumer) {
    this.hole = consumer;
  }

  @Override
  public void onRequestNext(GlobalTime from) {
    //final GlobalTime globalTime = history.ceilingKey(from);
    //if (globalTime == null) {
    //  unsatisfiedRequest = from;
    //} else {
    //  assert hole != null;
    //  hole.accept(new PayloadDataItem<>(Meta.meta(globalTime), history.get(globalTime)));
    //  unsatisfiedRequest = null;
    //}
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    history.headMap(to).clear();
  }

  private void onRaw(RawData<T> data) {
    hole.accept(new PayloadDataItem<>(Meta.meta(currentTime()), data.data()));
    //history.put(currentTime(), data.data());
    //
    //if (unsatisfiedRequest != null) {
    //  onRequestNext(unsatisfiedRequest);
    //}
  }

  private void emmitHeartbeat() {
    if (hole != null) {
      hole.accept(currentTime());
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

  public static class RawData<T> {
    private final T data;

    public RawData(T data) {
      this.data = data;
    }

    public T data() {
      return data;
    }
  }
}