package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.front.api.NewHole;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class FrontActor<T extends Front> extends LoggingActor {
  private final String id;
  private final T front;

  private FrontActor(String nodeId, FrontInstance<T> frontInstance) throws
                                                                    IllegalAccessException,
                                                                    InvocationTargetException,
                                                                    InstantiationException {
    this.id = frontInstance.name() + '-' + nodeId;
    final List<String> args = Arrays.asList(frontInstance.args());
    args.add(0, id);

    this.front = (T) frontInstance.front().getDeclaredConstructors()[0]
            .newInstance(args.toArray());
  }

  public static <T extends Front> Props props(String nodeId, FrontInstance<T> frontInstance) {
    return Props.create(FrontActor.class, nodeId, frontInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(NewHole.class, hole -> {
              front.onStart(new Backdoor(context(), item -> hole.source().tell(item, self())));
              front.onRequestNext(hole.lower());
            })
            .build();
  }

  public static class Backdoor implements Consumer<Object> {
    private final akka.actor.ActorContext context;
    private final Consumer<Object> realConsumer;

    public Backdoor(akka.actor.ActorContext context, Consumer<Object> realConsumer) {
      this.context = context;
      this.realConsumer = realConsumer;
    }

    @Override
    public void accept(Object o) {
      realConsumer.accept(o);
    }

    public akka.actor.ActorContext context() {
      return context;
    }
  }
}
