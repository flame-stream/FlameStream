package com.spbsu.flamestream.runtime.environment.remote;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.atomic.impl.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.atomic.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.runtime.graph.AtomicHandleImpl;
import com.spbsu.flamestream.runtime.raw.SingleRawData;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.ToIntFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class RemoteActorSink<T> extends AbstractAtomicGraph {
  private final ActorPath path;
  private final InPort inPort;
  @Nullable
  private ActorSelection actor = null;

  public RemoteActorSink(ToIntFunction<? super T> hashFunction, ActorPath receiverPath) {
    this.path = receiverPath;
    this.inPort = new InPort(hashFunction);
  }

  @Override
  public void onStart(AtomicHandle handle) {
    actor = ((AtomicHandleImpl) handle).backdoor().actorSelection(path);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    assert actor != null;
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
