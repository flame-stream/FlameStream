package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.tick.TickInfo;
import com.spbsu.datastream.core.range.atomic.AtomicActor;
import com.spbsu.datastream.core.range.atomic.AtomicHandleImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class RangeConcierge extends LoggingActor {
  private final TickInfo tickInfo;
  private final ActorRef dns;

  private final Map<AtomicGraph, ActorRef> initializedGraph;

  private final Map<InPort, ActorRef> routingTable;

  private RangeConcierge(final TickInfo tickInfo, final ActorRef dns) {
    this.tickInfo = tickInfo;
    this.dns = dns;
    this.initializedGraph = this.initializedAtomics(tickInfo.graph().graph().subGraphs());

    // TODO: 5/8/17 akka routing
    this.routingTable = RangeConcierge.withFlattenedKey(this.initializedGraph);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof PortBindDataItem) {
      final PortBindDataItem portBindDataItem = (PortBindDataItem) message;
      final ActorRef route = this.routingTable.getOrDefault(portBindDataItem.inPort(), this.context().system().deadLetters());
      route.tell(message, ActorRef.noSender());
    } else if (message instanceof MinTimeUpdate) {
      this.routingTable.values().forEach(atomic -> atomic.tell(message, ActorRef.noSender()));
    } else {
      this.unhandled(message);
    }
  }

  public static Props props(final TickInfo info, final ActorRef dns) {
    return Props.create(RangeConcierge.class, info, dns);
  }

  private static Map<InPort, ActorRef> withFlattenedKey(final Map<AtomicGraph, ActorRef> map) {
    final Map<InPort, ActorRef> result = new HashMap<>();
    for (final Map.Entry<AtomicGraph, ActorRef> e : map.entrySet()) {
      for (final InPort port : e.getKey().inPorts()) {
        result.put(port, e.getValue());
      }
    }
    return result;
  }

  private Map<AtomicGraph, ActorRef> initializedAtomics(final Collection<? extends AtomicGraph> atomicGraphs) {
    return atomicGraphs.stream().collect(Collectors.toMap(Function.identity(), this::actorForAtomic));
  }

  private ActorRef actorForAtomic(final AtomicGraph atomic) {
    final String id = UUID.randomUUID().toString();
    // TODO: 5/8/17 WIRE DB
    return this.context().actorOf(AtomicActor.props(atomic, new AtomicHandleImpl(this.tickInfo, this.dns, null, this.context())), id);
  }
}
