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

public class AkkaRearType<T> implements FlameRuntime.RearType<AkkaRear, AkkaRear.Handle<T>> {
  private final ActorSystem system;
  private final Class<T> clazz;
  private final Map<EdgeContext, AkkaRear.Handle<T>> localHandles = new HashMap<>();

  public AkkaRearType(ActorSystem system, Class<T> clazz) {
    this.system = system;
    this.clazz = clazz;
  }

  @Override
  public FlameRuntime.RearInstance<AkkaRear> instance() {
    final Address address = system.provider().getDefaultAddress();
    final ActorPath path = RootActorPath.apply(address, "/").child("user");
    return new AkkaRearInstance(path);
  }

  @Override
  public AkkaRear.Handle<T> handle(EdgeContext context) {
    if (!localHandles.containsKey(context)) {
      final ActorRef localMediator = system.actorOf(
              AkkaRear.LocalMediator.props(clazz),
              context.edgeId().nodeId() + "-localrear"
      );
      final AkkaRear.Handle<T> rearHandle = new AkkaRear.Handle<>(localMediator);
      localHandles.put(context, rearHandle);
    }
    return localHandles.get(context);
  }

  private static class AkkaRearInstance implements FlameRuntime.RearInstance<AkkaRear> {
    private final ActorPath path;

    private AkkaRearInstance(ActorPath path) {
      this.path = path;
    }

    @Override
    public Class<AkkaRear> clazz() {
      return AkkaRear.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{path.toSerializationFormat()};
    }
  }
}
