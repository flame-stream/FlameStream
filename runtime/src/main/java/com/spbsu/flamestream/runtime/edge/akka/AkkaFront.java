package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AkkaFront implements Front {
  private final ActorRef innerActor;

  public AkkaFront(EdgeContext edgeContext, ActorRefFactory refFactory, String localMediatorPath) {
    this.innerActor = refFactory.actorOf(
            RemoteMediator.props(localMediatorPath + "/" + edgeContext.edgeId().nodeId() + "-local")
                    .withDispatcher("util-dispatcher"),
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
      final ActorPath path = ActorPaths.fromString(localMediatorPath);
      localMediator = AwaitResolver.resolve(path, context())
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
    private final NavigableMap<GlobalTime, DataItem> log = new TreeMap<>();
    private final EdgeContext edgeContext;

    private boolean producerWait = false;
    private GlobalTime lastEmitted = GlobalTime.MIN;
    private long time = 0;

    private int requestDebt;
    private ActorRef sender;
    private ActorRef remoteMediator;

    private LocalMediator(EdgeContext edgeContext, boolean backPressure) {
      this.edgeContext = edgeContext;
      if (backPressure) {
        requestDebt = 0;
      } else {
        // This can cause overflow, there is a protection below
        requestDebt = Integer.MAX_VALUE;
      }
    }

    public static Props props(EdgeContext context, boolean backPressure) {
      return Props.create(LocalMediator.class, context, backPressure);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Start.class, start -> {
                onStart(start);
                unstashAll();
                getContext().become(processing());
              })
              .matchAny(o -> stash())
              .build();
    }

    private Receive processing() {
      return ReceiveBuilder.create()
              .match(Start.class, this::onStart)
              .match(RequestNext.class, r -> {
                // Overflow protection
                requestDebt = Math.max(requestDebt + 1, requestDebt);
                if (producerWait) {
                  process();
                  producerWait = false;
                }
              })
              .match(Checkpoint.class, checkpoint -> log.headMap(checkpoint.time()).clear())
              .match(Raw.class, raw -> {
                sender = sender();
                final GlobalTime globalTime = new GlobalTime(++time, edgeContext.edgeId());
                log.put(globalTime, new PayloadDataItem(new Meta(globalTime), raw.raw));
                if (requestDebt > 0) {
                  process();
                } else {
                  producerWait = true;
                }
              })
              .match(
                      Command.class,
                      command -> command == Command.EOS,
                      command -> {
                        remoteMediator.tell(new Heartbeat(new GlobalTime(
                                Long.MAX_VALUE,
                                edgeContext.edgeId()
                        )), self());
                        sender().tell(Command.OK, self());
                      }
              )
              .match(
                      Command.class,
                      command -> command == Command.UNREGISTER,
                      command -> {
                        remoteMediator.tell(new UnregisterFront(edgeContext.edgeId()), self());
                        sender().tell(Command.OK, self());
                      }
              )
              .build();
    }

    private void onStart(Start start) {
      remoteMediator = start.hole();
      time = Math.max(start.from().time(), time);
      lastEmitted = start.from();
    }

    private void process() {
      assert requestDebt > 0;
      requestDebt--;

      final Map.Entry<GlobalTime, DataItem> entry = log.higherEntry(lastEmitted);
      remoteMediator.tell(entry.getValue(), self());
      remoteMediator.tell(new Heartbeat(entry.getKey()), self());

      lastEmitted = entry.getKey();
      sender.tell(Command.OK, self());
    }
  }

  public static class FrontHandle<T> implements Consumer<T> {
    private static final Timeout TIMEOUT = Timeout.apply(60, TimeUnit.SECONDS);
    private final ActorRef localMediator;

    public FrontHandle(ActorRef localMediator) {
      this.localMediator = localMediator;
    }

    @Override
    public void accept(T value) {
      try {
        PatternsCS.ask(localMediator, new Raw(value), TIMEOUT).toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    public void eos() {
      try {
        PatternsCS.ask(localMediator, Command.EOS, TIMEOUT).toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    public void unregister() {
      try {
        PatternsCS.ask(localMediator, Command.UNREGISTER, TIMEOUT).toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class Raw {
    private final Object raw;

    private Raw(Object raw) {
      this.raw = raw;
    }
  }

  private enum Command {
    EOS,
    UNREGISTER,
    OK
  }
}