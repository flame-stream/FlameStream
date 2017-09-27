package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.barrier.PreBarrierMetaElement;
import com.spbsu.flamestream.runtime.raw.SingleRawData;
import com.typesafe.config.ConfigFactory;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class RemoteActorSink extends AbstractAtomicGraph {
  private final ActorPath path;

  private ActorSystem system;

  @Nullable
  private ActorSelection actor = null;

  private final InPort inPort;

  public RemoteActorSink(ActorPath receiverPath) {
    this.path = receiverPath;
    this.inPort = new InPort(PreBarrierMetaElement.HASH_FUNCTION);
  }

  @Override
  public void onStart(AtomicHandle handle) {
    final String id = UUID.randomUUID().toString();
    final int port = new Random(System.nanoTime()).nextInt(10000) + 30000;
    system = ActorSystem.create(
            "sink-system-" + id,
            ConfigFactory
                    .parseString("akka.remote.netty.tcp.port=" + port)
                    .withFallback(ConfigFactory.load("remote")));
    actor = system.actorSelection(path);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final PreBarrierMetaElement<?> element = (PreBarrierMetaElement<?>) item.payload();
    assert actor != null;
    actor.tell(new SingleRawData<>(element.payload()), ActorRef.noSender());
  }

  public InPort inPort() {
    return inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return emptyList();
  }
}
