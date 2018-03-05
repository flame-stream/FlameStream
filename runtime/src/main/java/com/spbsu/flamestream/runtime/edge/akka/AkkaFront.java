package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AkkaFront implements Front {
  private final ActorRef innerActor;

  public AkkaFront(EdgeContext edgeContext, ActorRefFactory refFactory, String localMediatorPath) {
    this.innerActor = refFactory.actorOf(
            RemoteMediator.props(localMediatorPath + "/" + edgeContext.edgeId().nodeId() + "-local"),
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
    private final String localMediatorPath;
    private ActorRef localMediator;
    private Consumer<Object> hole = null;

    private RemoteMediator(String localMediatorPath) {
      this.localMediatorPath = localMediatorPath;
    }

    public static Props props(String localMediatorPath) {
      return Props.create(RemoteMediator.class, localMediatorPath);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      localMediator = AwaitResolver.resolve(ActorPaths.fromString(localMediatorPath), context())
              .toCompletableFuture()
              .get();
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(MediatorStart.class, s -> {
                hole = s.hole;
                localMediator.tell(new Start(self(), s.from), self());
              })
              .match(DataItem.class, t2 -> hole.accept(t2))
              .match(Heartbeat.class, t1 -> hole.accept(t1))
              .match(UnregisterFront.class, t -> hole.accept(t))
              .match(RequestNext.class, r -> localMediator.tell(r, self()))
              .match(Checkpoint.class, c -> localMediator.tell(c, self()))
              .build();
    }

  }

  public static class MediatorStart {
    final Consumer<Object> hole;
    final GlobalTime from;

    MediatorStart(Consumer<Object> hole, GlobalTime from) {
      this.hole = hole;
      this.from = from;
    }
  }

  public static class LocalMediator extends LoggingActor {
    private final EdgeContext edgeContext;
    private final BlockingQueue<Object> queue;

    private long time = 0;
    private int requestDebt;

    private ActorRef remoteMediator;
    private Cancellable ping;

    private LocalMediator(EdgeContext edgeContext, BlockingQueue<Object> queue, boolean backPressure) {
      this.edgeContext = edgeContext;
      this.queue = queue;
      if (backPressure) {
        requestDebt = 0;
      } else {
        // This can cause overflow, there is a protection below
        requestDebt = Integer.MAX_VALUE;
      }
    }

    public static Props props(EdgeContext context, BlockingQueue<Object> queue, boolean backPressure) {
      return Props.create(LocalMediator.class, context, queue, backPressure);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      ping = context().system().scheduler().schedule(
              FiniteDuration.apply(1, TimeUnit.SECONDS),
              FiniteDuration.apply(1, TimeUnit.SECONDS),
              self(),
              "PING",
              context().system().dispatcher(),
              self()
      );
    }

    @Override
    public void postStop() {
      super.postStop();
      if (ping != null && !ping.isCancelled()) {
        ping.cancel();
      }
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Start.class, s -> {
                remoteMediator = s.hole();
                time = Math.max(time, s.from().time());
                tryDequeue();
              })
              .match(RequestNext.class, r -> {
                // Overflow protection
                requestDebt = Math.max(requestDebt + 1, requestDebt);
                tryDequeue();
              })
              .match(Checkpoint.class, c -> {})
              .match(String.class, s -> s.equals("PING"), ping -> tryDequeue())
              .build();
    }

    private void tryDequeue() {
      if (!queue.isEmpty() && remoteMediator != null && queue.peek() instanceof Command) {
        if (queue.peek() == Command.EOS) {
          queue.poll();
          remoteMediator.tell(new Heartbeat(new GlobalTime(Long.MAX_VALUE, edgeContext.edgeId())), self());
        } else if (queue.peek() == Command.UNREGISTER) {
          queue.poll();
          remoteMediator.tell(new UnregisterFront(edgeContext.edgeId()), self());
        }
      } else {
        //Fix this code if you feel the power
        while (requestDebt > 0 && !queue.isEmpty() && remoteMediator != null && !(queue.peek() instanceof Command)) {
          final Object p = queue.poll();
          final GlobalTime globalTime = new GlobalTime(++time, edgeContext.edgeId());
          remoteMediator.tell(new PayloadDataItem(new Meta(globalTime), p), self());
          remoteMediator.tell(new Heartbeat(globalTime), self());

          requestDebt--;
        }
      }
    }
  }

  public static class FrontHandle<T> implements Consumer<T> {
    private final BlockingQueue<Object> localMediator;

    public FrontHandle(BlockingQueue<Object> localMediator) {
      this.localMediator = localMediator;
    }

    @Override
    public void accept(T value) {
      try {
        localMediator.put(value);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void eos() {
      try {
        localMediator.put(Command.EOS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void unregister() {
      try {
        localMediator.put(Command.UNREGISTER);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private enum Command {
    EOS,
    UNREGISTER
  }
}