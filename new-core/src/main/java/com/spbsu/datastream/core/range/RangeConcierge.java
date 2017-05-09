package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Commit;
import com.spbsu.datastream.core.ack.CommitDone;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.range.atomic.AtomicActor;
import com.spbsu.datastream.core.range.atomic.AtomicHandleImpl;
import com.spbsu.datastream.core.range.atomic.PortBindDataItem;
import com.spbsu.datastream.core.tick.TickInfo;
import com.spbsu.datastream.core.tick.TickMessage;
import org.iq80.leveldb.DB;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class RangeConcierge extends LoggingActor {
  private final TickInfo tickInfo;
  private final ActorRef dns;
  private final HashRange myRange;

  private final Map<AtomicGraph, ActorRef> initializedGraph;
  private final Map<InPort, ActorRef> routingTable;
  private final DB db;

  private RangeConcierge(final TickInfo tickInfo,
                         final ActorRef dns,
                         final HashRange myRange,
                         final DB db) {
    this.tickInfo = tickInfo;
    this.db = db;
    this.dns = dns;
    this.initializedGraph = this.initializedAtomics(tickInfo.graph().graph().subGraphs());
    this.myRange = myRange;

    // TODO: 5/8/17 akka routing
    this.routingTable = RangeConcierge.withFlattenedKey(this.initializedGraph);
  }

  public static Props props(final TickInfo info, final ActorRef dns, final HashRange myRange,
                            final DB db) {
    return Props.create(RangeConcierge.class, info, dns, myRange, db);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received: {}", message);
    if (message instanceof PortBindDataItem) {
      final PortBindDataItem portBindDataItem = (PortBindDataItem) message;
      final ActorRef route = this.routingTable.getOrDefault(portBindDataItem.inPort(), this.context().system().deadLetters());
      route.tell(message, ActorRef.noSender());
    } else if (message instanceof MinTimeUpdate) {
      this.routingTable.values().forEach(atomic -> atomic.tell(message, ActorRef.noSender()));
    } else if (message instanceof Commit) {
      this.initializedGraph.values().forEach(atom -> atom.tell(message, ActorRef.noSender()));
    } else if (message instanceof AtomicCommitDone) {
      final AtomicCommitDone done = (AtomicCommitDone) message;
      this.processCommitDone(done.graph());
    } else {
      this.unhandled(message);
    }
  }

  private void processCommitDone(final AtomicGraph atomicGraph) {
    this.initializedGraph.remove(atomicGraph);
    if (this.initializedGraph.isEmpty()) {
      this.dns.tell(new UnresolvedMessage<>(this.tickInfo.ackerLocation(),
              new TickMessage<>(this.tickInfo.startTs(),
                      new CommitDone(this.myRange))), ActorRef.noSender());
      this.LOG().info("Commit done");
      this.context().stop(this.self());
    }
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
    return this.context().actorOf(AtomicActor.props(atomic, new AtomicHandleImpl(this.tickInfo, this.dns, this.db, this.context())), id);
  }
}
