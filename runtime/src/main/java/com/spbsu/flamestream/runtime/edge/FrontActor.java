package com.spbsu.flamestream.runtime.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor extends LoggingActor {
  private final Front front;

  private FrontActor(EdgeContext context, FlameRuntime.FrontInstance<?> frontInstance) throws
          IllegalAccessException,
          InvocationTargetException,
          InstantiationException {
    this.front = (Front) frontInstance.clazz().getDeclaredConstructors()[0]
            .newInstance(context);
  }

  public static Props props(EdgeContext context, FlameRuntime.FrontInstance<?> frontInstance) {
    return Props.create(FrontActor.class, context, frontInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Start.class, hole -> front.onStart(item -> hole.consumer().tell(item, self()), hole.globalTime()))
            .match(RequestNext.class, request -> front.onRequestNext())
            .build();
  }
}
