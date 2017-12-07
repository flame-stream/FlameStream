package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.RawData;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;

import java.util.function.Consumer;

public class AkkaFrontType implements FlameRuntime.FrontType<AkkaFront, AkkaFrontType.Handle> {
  private final ActorSystem system;

  public AkkaFrontType(ActorSystem system) {
    this.system = system;
  }

  @Override
  public Class<AkkaFront> frontClass() {
    return AkkaFront.class;
  }

  @Override
  public Handle handle(EdgeContext context) {
    final ActorRef ref = AwaitResolver.syncResolve(
            context.nodePath().child("edge").child(context.edgeId() + "-inner"),
            system
    );
    return new Handle(ref);
  }

  public static class Handle implements Consumer<Object> {
    private final ActorRef ref;

    public Handle(ActorRef ref) {
      this.ref = ref;
    }

    @Override
    public void accept(Object o) {
      ref.tell(new RawData<>(o), ActorRef.noSender());
    }
  }
}

