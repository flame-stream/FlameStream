package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import akka.testkit.javadsl.TestKit;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameNode;
import com.spbsu.flamestream.runtime.acker.InMemoryRegistry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.config.NodeConfig;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class FlameNodeTest extends FlameStreamSuite {
  private ActorSystem system;

  @BeforeMethod
  public void setup() {
    system = ActorSystem.create();
  }

  @AfterMethod
  public void teardown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void testFrontAttachment() throws InterruptedException {
    final Object wait = new Object();
    final InMemoryRegistry registry = new InMemoryRegistry();

    final ActorRef flameNode = system.actorOf(FlameNode.props(
            "1",
            EMPTY_GRAPH,
            new ClusterConfig(Collections.singletonMap(
                    "1",
                    new NodeConfig(
                            RootActorPath.apply(Address.apply("akka", system.name()), "/").child("user").child("1"),
                            new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE)
                    )
            ), "1"),
            registry
    ), "1");

    flameNode.tell(
            new FrontInstance<>("1", AwaitingFront.class, Collections.singletonList(wait)),
            ActorRef.noSender()
    );
    synchronized (wait) {
      wait.wait(TimeUnit.SECONDS.toMillis(5));
    }

    Assert.assertFalse(registry.registry.isEmpty());
  }

  public static class AwaitingFront implements Front {
    private final Object wait;

    public AwaitingFront(String id, Object wait) {
      this.wait = wait;
    }

    @Override
    public void onStart(Consumer<?> consumer) {
      synchronized (wait) {
        wait.notifyAll();
      }
    }

    @Override
    public void onRequestNext(GlobalTime from) {
    }

    @Override
    public void onCheckpoint(GlobalTime to) {
    }
  }

  private static final Graph EMPTY_GRAPH;

  static {
    final Source source = new Source();
    final Sink sink = new Sink();
    EMPTY_GRAPH = new Graph.Builder().link(source, sink).build(source, sink);
  }
}