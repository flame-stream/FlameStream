package com.spbsu.datastream.core.tick.manager;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.tick.TickContext;
import com.spbsu.datastream.core.tick.atomic.AtomicActor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandleImpl;
import com.spbsu.datastream.core.node.RootRouterApi;
import com.spbsu.datastream.core.tick.TickLocalRouter;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.spbsu.datastream.core.tick.manager.TickGraphManagerApi.TickStarted;

public final class TickGraphManager extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private TickGraphManager(final TickContext context) {
    final Map<AtomicGraph, ActorRef> inMapping = this.initializedAtomics(context.graph().subGraphs(), context);
    final ActorRef localRouter = this.localRouter(TickGraphManager.withFlattenedKey(inMapping));

    context.rootRouter().tell(new RootRouterApi.RegisterMe(context.tick(), localRouter), this.self());
  }

  public static Props props(final TickContext context) {
    return Props.create(TickGraphManager.class, context);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting...");
    // TODO: 3/26/17 Shitty startup
    this.context().system().scheduler().scheduleOnce(
            FiniteDuration.apply(5, TimeUnit.SECONDS),
            this.self(),
            new TickStarted(),
            this.context().system().dispatcher(),
            this.self());
    super.preStart();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    this.LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG.debug("Received: {}", message);

    if (message instanceof TickStarted) {
      this.getContext().getChildren().forEach(actorRef -> actorRef.tell(new TickStarted(), ActorRef.noSender()));
    }
  }

  private ActorRef localRouter(final TLongObjectMap<ActorRef> portMappings) {
    this.LOG.info("Creating local router");
    return this.context().actorOf(TickLocalRouter.props(portMappings), "localRouter");
  }

  private static TLongObjectMap<ActorRef> withFlattenedKey(final Map<AtomicGraph, ActorRef> map) {
    final TLongObjectMap<ActorRef> result = new TLongObjectHashMap<>();
    for (final Map.Entry<AtomicGraph, ActorRef> e : map.entrySet()) {
      for (final InPort port : e.getKey().inPorts()) {
        result.put(port.id(), e.getValue());
      }
    }
    return result;
  }

  private Map<AtomicGraph, ActorRef> initializedAtomics(final Collection<? extends AtomicGraph> atomicGraphs,
                                                        final TickContext context) {
    return atomicGraphs.stream().collect(Collectors.toMap(Function.identity(), a -> this.actorForAtomic(a, context)));
  }

  private ActorRef actorForAtomic(final AtomicGraph atomic, final TickContext context) {
    this.LOG.info("Creating actor for atomic {}", atomic);

    final String id = UUID.randomUUID().toString();
    return this.context().actorOf(AtomicActor.props(atomic, new AtomicHandleImpl(context), id), id);
  }
}
