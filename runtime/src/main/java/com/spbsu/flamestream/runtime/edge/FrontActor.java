package com.spbsu.flamestream.runtime.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor extends LoggingActor {
  private final Front front;

  private FrontActor(EdgeId edgeId, FlameRuntime.FrontInstance<?> frontInstance) throws
          IllegalAccessException,
          InvocationTargetException,
          InstantiationException {
    //handle AkkaFront in order to not create yet another actor system
    if (frontInstance.clazz().equals(AkkaFront.class)) {
      this.front = (Front) frontInstance.clazz().getDeclaredConstructors()[0].newInstance(edgeId, context());
    } else {
      this.front = (Front) frontInstance.clazz().getDeclaredConstructors()[0].newInstance(frontInstance.params());
    }
  }

  public static Props props(EdgeId edgeId, FlameRuntime.FrontInstance<?> frontInstance) {
    return Props.create(FrontActor.class, edgeId, frontInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Start.class, hole -> front.onStart(item -> hole.consumer().tell(item, self()), hole.globalTime()))
            .match(RequestNext.class, request -> front.onRequestNext())
            .build();
  }
}
