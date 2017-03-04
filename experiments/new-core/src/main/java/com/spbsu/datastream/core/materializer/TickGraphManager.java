package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ShardMappedGraph;
import scala.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TickGraphManager extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private final TickContext context;
  private final ShardMappedGraph graph;

  public static Props props(final TickContext context, final ShardMappedGraph graph) {
    return Props.create(TickGraphManager.class, context, graph);
  }

  private TickGraphManager(final TickContext context, final ShardMappedGraph graph) {
    this.context = context;
    this.graph = graph;
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");

    final Map<AtomicGraph, ActorRef> inMapping = actorsOf(graph.subGraphs());
    registerInnerPorts(flatKey(inMapping), graph.upstreams());

    inMapping.values().forEach(actorRef -> actorRef.tell(new TickGraphManagerApi.Start(), getSelf()));
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.warning("Restarting, reason: {}, message: {}", reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    LOG.debug("Recived: {}", message);
  }

  private void registerInnerPorts(final Map<InPort, ActorRef> portMappings, final Map<InPort, OutPort> upstreams) {
    portMappings.forEach((inPort, actorRef) -> {
      final OutPort out = upstreams.get(inPort);
      context.localLocator().registerPort(out, new AddressingSink(actorRef, inPort));
    });
  }

  private Map<InPort, ActorRef> flatKey(final Map<AtomicGraph, ActorRef> map) {
    final Map<InPort, ActorRef> result = new HashMap<>();
    for (Map.Entry<AtomicGraph, ActorRef> e : map.entrySet()) {
      for (InPort port : e.getKey().inPorts()) {
        result.put(port, e.getValue());
      }
    }
    return result;
  }

  private Map<AtomicGraph, ActorRef> actorsOf(final Set<? extends AtomicGraph> atomicGraph) {
    return atomicGraph.stream().collect(Collectors.toMap(Function.identity(), this::actorForAtomic));
  }

  private ActorRef actorForAtomic(final AtomicGraph atomic) {
    LOG.info("Creating actor for atomic {}", atomic);
    GraphStageLogic logic = atomic.logic();
    logic.injectConcierge(context);
    return context().actorOf(AtomicActor.props(logic), atomic.logic().toString() + UUID.randomUUID().toString());
  }
}
