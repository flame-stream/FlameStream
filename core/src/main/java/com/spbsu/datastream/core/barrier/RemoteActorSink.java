package com.spbsu.datastream.core.barrier;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
import com.spbsu.datastream.core.raw.SingleRawData;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class RemoteActorSink extends AbstractAtomicGraph {
  private final ActorPath path;

  private ActorSelection actor;

  private final InPort inPort;

  public RemoteActorSink(ActorPath path) {
    this.path = path;
    this.inPort = new InPort(PreBarrierMetaElement.HASH_FUNCTION);
  }

  @Override
  public void onStart(AtomicHandle handle) {
    this.actor = handle.actorSelection(path);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final PreBarrierMetaElement<?> element = (PreBarrierMetaElement<?>) item.payload();
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
