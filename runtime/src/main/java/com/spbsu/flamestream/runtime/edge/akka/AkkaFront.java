package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.function.Consumer;

public class AkkaFront implements Front {
  private final ActorRef innerActor;

  public AkkaFront(EdgeId edgeId, ActorRefFactory refFactory) {
    this.innerActor = refFactory.actorOf(InnerActor.props(edgeId), edgeId.nodeId() + "-inner");
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {
    innerActor.tell(new AkkaStart(consumer, from), ActorRef.noSender());
  }

  @Override
  public void onRequestNext() {
    innerActor.tell(new RequestNext(), ActorRef.noSender());
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
  }

  private static class InnerActor extends LoggingActor {
    private final EdgeId frontId;

    private ActorRef frontHandle = null;
    private Consumer<Object> hole = null;
    private long prevGlobalTs = 0;

    private InnerActor(EdgeId frontId) {
      this.frontId = frontId;
    }

    public static Props props(EdgeId frontId) {
      return Props.create(InnerActor.class, frontId);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(RawData.class, this::onRaw)
              .match(AkkaStart.class, this::onStart)
              .match(RequestNext.class, this::onRequestNext)
              .match(EOS.class, s -> onEos())
              .build();
    }

    private void onEos() {
      if (hole != null) {
        hole.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, frontId)));
      } else {
        stash();
      }
    }

    private void onStart(AkkaStart start) {
      this.hole = start.consumer;
      unstashAll();
    }

    private void onRequestNext(RequestNext requestNext) {
      if (frontHandle != null && !frontHandle.equals(context().system().deadLetters())) {
        frontHandle.tell(requestNext, self());
      }
    }

    private void onRaw(RawData<Object> data) {
      if (hole != null) {
        final PayloadDataItem dataItem = new PayloadDataItem(new Meta(currentTime()), data.data());
        hole.accept(dataItem);
        hole.accept(new Heartbeat(currentTime()));
      } else {
        stash();
      }

      if (frontHandle == null) {
        frontHandle = sender();
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

  private static class AkkaStart {
    private final Consumer<Object> consumer;
    @SuppressWarnings("unused") //needs for replay
    private final GlobalTime globalTime;

    private AkkaStart(Consumer<Object> consumer, GlobalTime globalTime) {
      this.consumer = consumer;
      this.globalTime = globalTime;
    }
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

  static class EOS {
  }
}