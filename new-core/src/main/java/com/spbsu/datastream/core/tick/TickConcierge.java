package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.range.RangeRouterApi;
import com.spbsu.datastream.core.tick.atomic.AtomicActor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandleImpl;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TickConcierge extends LoggingActor {
  private TickConcierge(final TickContext context) {
    final Map<AtomicGraph, ActorRef> inMapping = this.initializedAtomics(context.graph().subGraphs(), context);

    final ActorRef localRouter = this.localRouter(TickConcierge.withFlattenedKey(inMapping));
    context.rangeRouter().tell(new RangeRouterApi.RegisterMe(context.tick(), localRouter), this.self());
  }

  public static Props props(final TickContext context) {
    return Props.create(TickConcierge.class, context);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting...");
    // TODO: 3/26/17 Shitty startup
    this.context().system().scheduler().scheduleOnce(
            FiniteDuration.apply(5, TimeUnit.SECONDS),
            this.self(),
            new TickConciergeApi.TickStarted(),
            this.context().system().dispatcher(),
            this.self());
    super.preStart();
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG.debug("Received: {}", message);

    if (message instanceof TickConciergeApi.TickStarted) {
      this.getContext().getChildren().forEach(actorRef -> actorRef.tell(new TickConciergeApi.TickStarted(), ActorRef.noSender()));
    }
  }

  private ActorRef localRouter(final Map<InPort, ActorRef> portMappings) {
    return this.context().actorOf(TickLocalRouter.props(portMappings), "localRouter");
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

  private Map<AtomicGraph, ActorRef> initializedAtomics(final Collection<? extends AtomicGraph> atomicGraphs,
                                                        final TickContext context) {
    return atomicGraphs.stream().collect(Collectors.toMap(Function.identity(), a -> this.actorForAtomic(a, context)));
  }

  private ActorRef actorForAtomic(final AtomicGraph atomic, final TickContext context) {
    final String id = UUID.randomUUID().toString();
    return this.context().actorOf(AtomicActor.props(atomic, new AtomicHandleImpl(context)), id);
  }
}
