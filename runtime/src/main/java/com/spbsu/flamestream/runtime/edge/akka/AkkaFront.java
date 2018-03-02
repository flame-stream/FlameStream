package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.function.Consumer;

public class AkkaFront implements Front {
  private final ActorRef innerActor;

  public AkkaFront(EdgeContext edgeContext, ActorRefFactory refFactory) {
    this.innerActor = refFactory.actorOf(
            RemoteMediator.props(),
            edgeContext.edgeId().nodeId() + "-inner"
    );
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {
    innerActor.tell(new MediatorStart(consumer, from), ActorRef.noSender());
  }

  @Override
  public void onRequestNext() {
    innerActor.tell(new RequestNext(), ActorRef.noSender());
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    innerActor.tell(new Checkpoint(to), ActorRef.noSender());
  }

  private static class RemoteMediator extends LoggingActor {
    private ActorRef remoteActor;
    private Consumer<Object> hole = null;

    public static Props props() {
      return Props.create(RemoteMediator.class);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(ActorRef.class, r -> {
                remoteActor = r;
                unstashAll();
                getContext().become(serving());
              })
              .matchAny(m -> stash())
              .build();
    }

    private Receive serving() {
      return ReceiveBuilder.create()
              .match(MediatorStart.class, s -> {
                hole = s.hole;
                remoteActor.tell(new Start(self(), s.from), self());
              })
              .match(DataItem.class, t2 -> hole.accept(t2))
              .match(Heartbeat.class, t1 -> hole.accept(t1))
              .match(UnregisterFront.class, t -> hole.accept(t))
              .match(RequestNext.class, r -> remoteActor.tell(r, self()))
              .match(Checkpoint.class, c -> remoteActor.tell(c, self()))
              .build();
    }

  }

  public static class MediatorStart {
    final Consumer<Object> hole;
    final GlobalTime from;

    public MediatorStart(Consumer<Object> hole, GlobalTime from) {
      this.hole = hole;
      this.from = from;
    }
  }

  public static class LocalMediator extends LoggingActor {
    private final Front front;
    private final ActorRef remoteMediator;

    private LocalMediator(Front front, ActorRef remoteMediator) {
      this.front = front;
      this.remoteMediator = remoteMediator;
    }

    public static Props props(Front front, ActorRef remoteActor) {
      return Props.create(LocalMediator.class, front, remoteActor);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      remoteMediator.tell(self(), self());
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Start.class, s -> {
                front.onStart(o -> s.hole().tell(o, self()), s.from());
              })
              .match(RequestNext.class, r -> front.onRequestNext())
              .match(Checkpoint.class, c -> front.onCheckpoint(c.time()))
              .build();
    }
  }
}