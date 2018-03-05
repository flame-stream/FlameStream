package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AkkaFrontType<T> implements FlameRuntime.FrontType<AkkaFront, AkkaFront.FrontHandle<T>> {
  private final ActorSystem system;
  private final boolean backPressure;
  private final Map<EdgeContext, AkkaFront.FrontHandle<T>> localHandles = new HashMap<>();

  public AkkaFrontType(ActorSystem system) {
    this(system, true);
  }

  public AkkaFrontType(ActorSystem system, boolean backPressure) {
    this.system = system;
    this.backPressure = backPressure;
  }

  @Override
  public FlameRuntime.FrontInstance<AkkaFront> instance() {
    final Address address = system.provider().getDefaultAddress();
    final ActorPath path = RootActorPath.apply(address, "/").child("user");

    return new FlameRuntime.FrontInstance<AkkaFront>() {
      @Override
      public Class<AkkaFront> clazz() {
        return AkkaFront.class;
      }

      @Override
      public String[] params() {
        return new String[]{path.toSerializationFormat()};
      }
    };
  }

  @Override
  public AkkaFront.FrontHandle<T> handle(EdgeContext context) {
    if (!localHandles.containsKey(context)) {
      final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
      system.actorOf(AkkaFront.LocalMediator.props(context, queue, backPressure), context.edgeId().nodeId() + "-local");

      final AkkaFront.FrontHandle<T> frontHandle = new AkkaFront.FrontHandle<>(queue);
      localHandles.put(context, frontHandle);
    }
    return localHandles.get(context);
  }
}
