package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.RawData;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;

import java.util.function.Consumer;

public class AkkaFrontType<T> implements FlameRuntime.FrontType<AkkaFront, AkkaFrontType.Handle<T>> {
  private final ActorSystem system;

  public AkkaFrontType(ActorSystem system) {
    this.system = system;
  }

  @Override
  public Class<AkkaFront> frontClass() {
    return AkkaFront.class;
  }

  @Override
  public Handle<T> handle(EdgeContext context) {
    final ActorRef ref = AwaitResolver.syncResolve(
            context.nodePath().child("edge").child(context.edgeId() + "-inner"),
            system
    );
    return new Handle<T>(ref);
  }

  public static class Handle<T> implements Consumer<T> {
    private final ActorRef ref;

    public Handle(ActorRef ref) {
      this.ref = ref;
    }

    @Override
    public void accept(T o) {
      ref.tell(new RawData<>(o), ActorRef.noSender());
    }
  }
}

