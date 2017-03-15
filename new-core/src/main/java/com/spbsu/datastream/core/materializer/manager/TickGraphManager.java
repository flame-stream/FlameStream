package com.spbsu.datastream.core.materializer.manager;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.materializer.TickContext;
import com.spbsu.datastream.core.materializer.atomic.AtomicActor;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandleImpl;
import com.spbsu.datastream.core.routing.RootRouterApi;
import com.spbsu.datastream.core.routing.TickLocalRouter;
import scala.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.spbsu.datastream.core.materializer.manager.TickGraphManagerApi.TickStarted;

public class TickGraphManager extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private TickGraphManager(final TickContext context) {

    final Map<AtomicGraph, ActorRef> inMapping = initializeAtomics(context.graph().subGraphs(), context);
    final ActorRef localRouter = localRouter(flatKey(inMapping));

    context.rootRouter().tell(new RootRouterApi.RegisterMe(context.tick(), localRouter), self());
  }

  public static Props props(final TickContext context) {
    return Props.create(TickGraphManager.class, context);
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");
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
      getContext().getChildren().forEach(actorRef -> actorRef.tell(new TickStarted(), getSelf()));
    }
  }

  private ActorRef localRouter(final Map<InPort, ActorRef> portMappings) {
    return context().actorOf(TickLocalRouter.props(portMappings), "localRouter");
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

  private Map<AtomicGraph, ActorRef> initializeAtomics(final Set<? extends AtomicGraph> atomicGraphs,
                                                       final TickContext context) {
    return atomicGraphs.stream().collect(Collectors.toMap(Function.identity(), a -> actorForAtomic(a, context)));
  }

  private ActorRef actorForAtomic(final AtomicGraph atomic, final TickContext context) {
    LOG.info("Creating actor for atomic {}", atomic);
    return context().actorOf(AtomicActor.props(atomic, new AtomicHandleImpl(context)), UUID.randomUUID().toString());
  }
}
