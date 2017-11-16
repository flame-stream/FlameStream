package com.spbsu.flamestream.runtime.environment.local;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.HashFunction;
import com.spbsu.flamestream.runtime.acker.api.CommitTick;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.environment.CollectingActor;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.node.tick.api.TickCommitDone;
import com.spbsu.flamestream.runtime.node.tick.TickConcierge;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
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

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

public final class LocalEnvironment implements Environment {
  private static final String SYSTEM_NAME = "local-system";

  private final ConcurrentMap<Long, ActorRef> tickConcierges = new ConcurrentHashMap<>();
  private final ActorRef fakeWatcher;
  private final ActorSystem localSystem;

  private ActorRef front;

  public LocalEnvironment() {
    this.localSystem = ActorSystem.create(SYSTEM_NAME, ConfigFactory.load("local"));
    this.fakeWatcher = localSystem.actorOf(FakeTickWatcher.props(tickConcierges), "fake-watcher");
  }

  @Override
  public void deploy(TickInfo tickInfo) {
    final ActorRef concierge = localSystem.actorOf(
            TickConcierge.props(
                    tickInfo,
                    "1",
                    singletonMap("1", localSystem.child(String.valueOf(tickInfo.id()))),
                    fakeWatcher
            ),
            String.valueOf(tickInfo.id())
    );

    front.tell(tickInfo, ActorRef.noSender());
    tickConcierges.put(tickInfo.id(), concierge);
  }

  @Override
  public void deployFront(String nodeId, String frontId, Props frontProps) {
    if (front == null) {
      front = localSystem.actorOf(frontProps, "front");
    } else {
      throw new IllegalStateException("There cannot be more than one front in local env");
    }
  }

  /*@Override
  public Set<Integer> availableFronts() {
    return singleton(1);
  }*/

  @Override
  public Set<String> availableWorkers() {
    return singleton("1");
  }

  @Override
  public <T> AtomicGraph wrapInSink(HashFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    return new LocalActorSink<>(hash, localSystem.actorOf(CollectingActor.props(mySuperConsumer), "collector"));
  }

  @Override
  public void awaitTick(long tickId) throws InterruptedException {
    Thread.sleep(10000);
  }

  @Override
  public Set<Long> ticks() {
    return tickConcierges.keySet();
  }

  /*@Override
  public Consumer<Object> frontConsumer(int frontId) {
    if (frontId == 1) {
      return object -> front.tell(new SingleRawData<>(object), ActorRef.noSender());
    } else {
      throw new IllegalArgumentException("oops");
    }
  }*/

  @Override
  public void close() {
    try {
      Await.ready(localSystem.terminate(), Duration.Inf());

      FileUtils.deleteDirectory(new File("leveldb"));
    } catch (InterruptedException | TimeoutException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class FakeTickWatcher extends LoggingActor {
    private final ConcurrentMap<Long, ActorRef> concierges;

    private FakeTickWatcher(ConcurrentMap<Long, ActorRef> concierges) {
      this.concierges = concierges;
    }

    static Props props(ConcurrentMap<Long, ActorRef> concierges) {
      return Props.create(FakeTickWatcher.class, concierges);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create().match(CommitTick.class, this::commitTick).matchAny(this::unhandled).build();
    }

    private void commitTick(CommitTick commit) {
      concierges.values().forEach(c -> c.tell(new TickCommitDone(commit.tickId()), self()));
    }
  }
}
