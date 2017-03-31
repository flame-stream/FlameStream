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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.spbsu.datastream.core.tick.manager.TickGraphManagerApi.TickStarted;

public class TickGraphManager extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private TickGraphManager(final TickContext context) {
    final Map<AtomicGraph, ActorRef> inMapping = initializedAtomics(context.graph().subGraphs(), context);
    final ActorRef localRouter = localRouter(withFlattenedKey(inMapping));

    context.rootRouter().tell(new RootRouterApi.RegisterMe(context.tick(), localRouter), self());
  }

  public static Props props(final TickContext context) {
    return Props.create(TickGraphManager.class, context);
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");
    // TODO: 3/26/17 Shitty startup
    context().system().scheduler().scheduleOnce(
            FiniteDuration.apply(5, TimeUnit.SECONDS),
            self(),
            new TickStarted(),
            context().system().dispatcher(),
            self());
    super.preStart();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    LOG.debug("Received: {}", message);

    if (message instanceof TickStarted) {
      getContext().getChildren().forEach(actorRef -> actorRef.tell(new TickStarted(), ActorRef.noSender()));
    }
  }

  private ActorRef localRouter(final TLongObjectMap<ActorRef> portMappings) {
    LOG.info("Creating local router");
    return context().actorOf(TickLocalRouter.props(portMappings), "localRouter");
  }

  private TLongObjectMap<ActorRef> withFlattenedKey(final Map<AtomicGraph, ActorRef> map) {
    final TLongObjectMap<ActorRef> result = new TLongObjectHashMap<>();
    for (Map.Entry<AtomicGraph, ActorRef> e : map.entrySet()) {
      for (InPort port : e.getKey().inPorts()) {
        result.put(port.id(), e.getValue());
      }
    }
    return result;
  }

  private Map<AtomicGraph, ActorRef> initializedAtomics(final Set<? extends AtomicGraph> atomicGraphs,
                                                        final TickContext context) {
    return atomicGraphs.stream().collect(Collectors.toMap(Function.identity(), a -> actorForAtomic(a, context)));
  }

  private ActorRef actorForAtomic(final AtomicGraph atomic, final TickContext context) {
    LOG.info("Creating actor for atomic {}", atomic);

    final String id = UUID.randomUUID().toString();
    return context().actorOf(AtomicActor.props(atomic, new AtomicHandleImpl(context), id), id);
  }
}
