package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import org.reactfx.collection.LiveList;

import java.util.HashMap;
import java.util.Map;

public class AkkaRearType<T> implements FlameRuntime.RearType<AkkaRear, AkkaRear.Handles<T>> {
  private final ActorSystem system;
  private final Class<T> clazz;
  private final Map<EdgeContext, ActorRef> localHandles = new HashMap<>();

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
  public AkkaRear.Handles<T> handles(LiveList<EdgeContext> contexts) {
    return new AkkaRear.Handles<>(contexts.map(context -> localHandles.computeIfAbsent(context, __ ->
            system.actorOf(
                    AkkaRear.LocalMediator.props(clazz),
                    context.edgeId().nodeId() + "-localrear"
            )
    )).memoize());
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
