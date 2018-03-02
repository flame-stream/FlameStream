package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class AkkaFrontType<T extends Front> implements FlameRuntime.FrontType<AkkaFront, T> {
  private final ActorSystem system;

  private final Function<EdgeContext, T> localFronts;

  public static <DATA_TYPE> AkkaFrontType<LocalFront<DATA_TYPE>> withLocalFront(ActorSystem system) {
    return new AkkaFrontType<>(new Function<EdgeContext, LocalFront<DATA_TYPE>>() {
      private final Map<EdgeContext, LocalFront<DATA_TYPE>> fronts = new HashMap<>();

      @Override
      public LocalFront<DATA_TYPE> apply(EdgeContext context) {
        fronts.putIfAbsent(context, new LocalFront<>(context));
        return fronts.get(context);
      }
    }, system);
  }

  public AkkaFrontType(Function<EdgeContext, T> localFronts, ActorSystem system) {
    this.system = system;
    this.localFronts = localFronts;
  }

  @Override
  public FlameRuntime.FrontInstance<AkkaFront> instance() {
    return new FlameRuntime.FrontInstance<AkkaFront>() {
      @Override
      public Class<AkkaFront> clazz() {
        return AkkaFront.class;
      }

      @Override
      public String[] params() {
        return new String[0];
      }
    };
  }

  @Override
  public T handle(EdgeContext context) {
    final ActorRef frontRef = AwaitResolver.syncResolve(
            context.nodePath()
                    .child("edge")
                    .child(context.edgeId().edgeName())
                    .child(context.edgeId().nodeId() + "-inner"),
            system
    );
    final T front = localFronts.apply(context);
    system.actorOf(AkkaFront.LocalMediator.props(front, frontRef));
    return front;
  }
}
