package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import akka.testkit.javadsl.TestKit;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.FlameNode;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.config.NodeConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

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
  public void testBootstrap() {
    final ActorRef flameNode = system.actorOf(FlameNode.props(
            "1",
            (frontId, attachTimestamp) -> {},
            new ClusterConfig(Collections.singletonMap(
                    "1",
                    new NodeConfig(
                            RootActorPath.apply(Address.apply("akka", system.name()), "1"),
                            new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE)
                    )
            ), "1")
    ), "1");

    flameNode.tell(new Graph.Builder(), ActorRef.noSender());
  }
}