package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ShardMappedGraph;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtomicManager extends UntypedActor {
  private final ShardConcierge concierge;

  private final ShardMappedGraph graph;

  private AtomicManager(final ShardConcierge concierge, final ShardMappedGraph graph) {
    this.concierge = concierge;
    this.graph = graph;
  }

  @Override
  public void preStart() throws Exception {
    final Map<AtomicGraph, ActorRef> inMapping = graph.subGraphs().stream()
            .collect(Collectors.toMap(Function.identity(), this::actorForAtomic));
    inMapping.forEach((atomic, actor) ->
            atomic.inPorts().forEach(inPort -> {
              final OutPort out = graph.upstreams().get(inPort);
              concierge.portLocator().registerPort(out, new BindConsumer(actor, inPort));
            }));
  }

  @Override
  public void onReceive(final Object message) throws Throwable {

  }

  private ActorRef actorForAtomic(final AtomicGraph atomic) {
    return context().actorOf(AtomicActor.props(atomic.logic()));
  }

  private static class BindConsumer implements Consumer<Object> {
    private final ActorRef actorRef;
    private final InPort portLabel;

    private BindConsumer(final ActorRef actorRef, final InPort portLabel) {
      this.actorRef = actorRef;
      this.portLabel = portLabel;
    }

    @Override
    public void accept(final Object o) {
      actorRef.tell(new AddressedMessage<>(o, portLabel), null);
    }
  }
}
