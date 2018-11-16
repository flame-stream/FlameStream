package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import javafx.collections.FXCollections;
import javafx.collections.ObservableSet;
import javafx.collections.SetChangeListener;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
  public AkkaRear.Handles<T> handles(ObservableSet<EdgeContext> contexts) {
    ObservableSet<ActorRef> localMediators =
            FXCollections.observableSet(contexts.stream()
                    .map(this::localMediator)
                    .collect(Collectors.toSet()));
    contexts.addListener((SetChangeListener<EdgeContext>) change -> {
      if (change.wasAdded()) {
        localMediators.add(localMediator(change.getElementAdded()));
      }
    });
    return new AkkaRear.Handles<>(FXCollections.unmodifiableObservableSet(localMediators));
  }

  private ActorRef localMediator(EdgeContext context) {
    return localHandles.computeIfAbsent(context, __ ->
            system.actorOf(
                    AkkaRear.LocalMediator.props(clazz),
                    context.edgeId().nodeId() + "-localrear"
            )
    );
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
