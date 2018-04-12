package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

import java.util.HashMap;
import java.util.Map;

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
    return new AkkaFrontInstance(path);
  }

  @Override
  public AkkaFront.FrontHandle<T> handle(EdgeContext context) {
    if (!localHandles.containsKey(context)) {
      final ActorRef localMediator = system.actorOf(
              AkkaFront.LocalMediator.props(context, backPressure),
              context.edgeId().nodeId() + "-local"
      );
      final AkkaFront.FrontHandle<T> frontHandle = new AkkaFront.FrontHandle<>(localMediator);
      localHandles.put(context, frontHandle);
    }
    return localHandles.get(context);
  }

  private static class AkkaFrontInstance implements FlameRuntime.FrontInstance<AkkaFront> {
    private final ActorPath path;

    private AkkaFrontInstance(ActorPath path) {
      this.path = path;
    }

    @Override
    public Class<AkkaFront> clazz() {
      return AkkaFront.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{path.toSerializationFormat()};
    }
  }
}
