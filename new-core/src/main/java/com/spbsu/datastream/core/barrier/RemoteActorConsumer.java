package com.spbsu.datastream.core.barrier;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;

public final class RemoteActorConsumer<T> extends AbstractAtomicGraph {
  private final ActorPath path;
  private final InPort inPort;

  private final BarrierCollector collector = new LinearCollector();

  private ActorSelection actor;

  public RemoteActorConsumer(final ActorPath path) {
    this.path = path;
    this.inPort = new InPort(PreSinkMetaElement.HASH_FUNCTION);
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public void onStart(final AtomicHandle handle) {
    this.actor = handle.actorSelection(this.path);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    this.collector.enqueue(item);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onMinGTimeUpdate(final GlobalTime globalTime, final AtomicHandle handle) {
    this.collector.update(globalTime);
    this.collector.release(di -> this.consume((DataItem<PreSinkMetaElement<T>>) di));
  }

  private void consume(final DataItem<PreSinkMetaElement<T>> di) {
    this.actor.tell(di.payload().payload(), ActorRef.noSender());
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.emptyList();
  }
}

