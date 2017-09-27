package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.barrier.PreBarrierMetaElement;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.data.raw.SingleRawData;
import com.typesafe.config.Config;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class RemoteActorSink extends AbstractAtomicGraph {
  private final ActorSystem system;
  private final ActorPath path;

  @Nullable
  private ActorSelection actor = null;

  private final InPort inPort;

  public RemoteActorSink(ActorPath path, Config config) {
    this.path = path;
    final String id = UUID.randomUUID().toString();
    this.system = ActorSystem.create("sink-system-" + id, config);
    this.inPort = new InPort(PreBarrierMetaElement.HASH_FUNCTION);
  }

  @Override
  public void onStart(AtomicHandle handle) {
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
