package com.spbsu.flamestream.runtime.environment.local;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.runtime.environment.raw.SingleRawData;

import java.util.List;
import java.util.function.ToIntFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

class LocalActorSink<T> extends AbstractAtomicGraph {
  private final ActorRef actor;
  private final InPort inPort;

  LocalActorSink(ToIntFunction<? super T> hash, ActorRef actor) {
    this.actor = actor;
    this.inPort = new InPort(hash);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    actor.tell(new SingleRawData<>(item.payload()), ActorRef.noSender());
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
