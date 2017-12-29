package com.spbsu.flamestream.runtime.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor extends LoggingActor {
  private final Front front;

  private FrontActor(EdgeContext edgeContext, FlameRuntime.FrontInstance<?> frontInstance) throws
          IllegalAccessException,
          InvocationTargetException,
          InstantiationException {
    final Object[] params;
    //handle AkkaFront in order to not create yet another actor system
    if (frontInstance.clazz().equals(AkkaFront.class)) {
      params = new Object[frontInstance.params().length + 2];
      params[0] = edgeContext;
      params[1] = context();
      System.arraycopy(frontInstance.params(), 0, params, 2, frontInstance.params().length);
    } else {
      params = new Object[frontInstance.params().length + 1];
      params[0] = edgeContext;
      System.arraycopy(frontInstance.params(), 0, params, 1, frontInstance.params().length);
    }
    front = (Front) frontInstance.clazz().getDeclaredConstructors()[0].newInstance(params);
  }

  public static Props props(EdgeContext edgeContext, FlameRuntime.FrontInstance<?> frontInstance) {
    return Props.create(FrontActor.class, edgeContext, frontInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Start.class, hole -> front.onStart(item -> hole.consumer().tell(item, self()), hole.globalTime()))
            .match(RequestNext.class, request -> front.onRequestNext())
            .build();
  }
}
