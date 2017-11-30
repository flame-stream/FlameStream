package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.function.Consumer;

public class RemoteActorFront implements Front {
  private final String id;
  private final String path;

  public RemoteActorFront(String id, String path) {
    this.id = id;
    this.path = path;
  }

  @Override
  public void onStart(Consumer<?> consumer) {
    ((FrontActor.Backdoor) consumer).context().actorOf(InnerActor.props(null, null));
  }

  @Override
  public void onRequestNext(GlobalTime from) {

  }

  @Override
  public void onCheckpoint(GlobalTime to) {

  }

  private static class InnerActor extends LoggingActor {
    private final String remoteActor;
    private final Consumer<Object> consumer;

    private InnerActor(String remoteActor, Consumer<Object> consumer) {
      this.remoteActor = remoteActor;
      this.consumer = consumer;
    }

    public static Props props(String remoteActor, Consumer<Object> consumer) {
      return Props.create(InnerActor.class, remoteActor, consumer);
    }

    @Override
    public void preStart() throws Exception {
      context().actorSelection(remoteActor).tell(self(), self());
      super.preStart();
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .build();
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
}