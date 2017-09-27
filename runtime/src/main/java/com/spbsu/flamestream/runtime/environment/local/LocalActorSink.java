package com.spbsu.flamestream.runtime.environment.local;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.barrier.PreBarrierMetaElement;
import com.spbsu.flamestream.runtime.raw.SingleRawData;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class LocalActorSink extends AbstractAtomicGraph {
  private final ActorRef actor;

  private final InPort inPort;

  public LocalActorSink(ActorRef actor) {
    this.actor = actor;
    this.inPort = new InPort(PreBarrierMetaElement.HASH_FUNCTION);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final PreBarrierMetaElement<?> element = (PreBarrierMetaElement<?>) item.payload();
    actor.tell(new SingleRawData<>(element.payload()), ActorRef.noSender());
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
