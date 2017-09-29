package com.spbsu.flamestream.runtime.environment.local;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.ack.CommitTick;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.environment.CollectingActor;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.front.FrontActor;
import com.spbsu.flamestream.runtime.raw.SingleRawData;
import com.spbsu.flamestream.runtime.tick.TickCommitDone;
import com.spbsu.flamestream.runtime.tick.TickConcierge;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

public final class LocalEnvironment implements Environment {
  private static final String SYSTEM_NAME = "local-system";

  private final ConcurrentMap<Long, ActorRef> tickConcierges = new ConcurrentHashMap<>();
  private final ActorRef fakeWatcher;
  private final ActorSystem localSystem;

  private final ActorRef front;

  public LocalEnvironment() {
    this.localSystem = ActorSystem.create(SYSTEM_NAME, ConfigFactory.load("remote"));
    this.fakeWatcher = localSystem.actorOf(FakeTickWatcher.props(tickConcierges), "fake-watcher");

    final ActorPath myPath = RootActorPath.apply(Address.apply("akka", SYSTEM_NAME), "/").child("user");
    this.front = localSystem.actorOf(FrontActor.props(singletonMap(1, myPath), 1), "front");
  }

  @Override
  public void deploy(TickInfo tickInfo) {
    final ActorRef concierge = localSystem.actorOf(
            TickConcierge.props(
                    tickInfo,
                    1,
                    singletonMap(1, localSystem.child(String.valueOf(tickInfo.id()))),
                    fakeWatcher
            ),
            String.valueOf(tickInfo.id())
    );

    front.tell(tickInfo, ActorRef.noSender());
    tickConcierges.put(tickInfo.id(), concierge);
  }

  @Override
  public Set<Integer> availableFronts() {
    return singleton(1);
  }

  @Override
  public Set<Integer> availableWorkers() {
    return singleton(1);
  }

  @Override
  public <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    return new LocalActorSink<>(hash, localSystem.actorOf(CollectingActor.props(mySuperConsumer), "collector"));
  }

  @Override
  public Consumer<Object> frontConsumer(int frontId) {
    if (frontId == 1) {
      return object -> front.tell(new SingleRawData<>(object), ActorRef.noSender());
    } else {
      throw new IllegalArgumentException("oops");
    }
  }

  @Override
  public void close() {
    try {
      Await.ready(localSystem.terminate(), Duration.Inf());

      FileUtils.deleteDirectory(new File("leveldb"));
    } catch (InterruptedException | TimeoutException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final static class FakeTickWatcher extends LoggingActor {
    private final ConcurrentMap<Long, ActorRef> concierges;

    private FakeTickWatcher(ConcurrentMap<Long, ActorRef> concierges) {
      this.concierges = concierges;
    }

    public static Props props(ConcurrentMap<Long, ActorRef> concierges) {
      return Props.create(FakeTickWatcher.class, concierges);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(CommitTick.class, this::commitTick)
              .matchAny(this::unhandled)
              .build();
    }

    private void commitTick(CommitTick commit) {
      concierges.values().forEach(c -> c.tell(new TickCommitDone(commit.tickId()), self()));
    }
  }
}
