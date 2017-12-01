package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.front.api.OnStart;
import com.spbsu.flamestream.runtime.edge.front.api.RequestNext;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor<T extends Front> extends LoggingActor {
  private final T front;

  private FrontActor(FrontInstance<T> frontInstance) throws
                                                     IllegalAccessException,
                                                     InvocationTargetException,
                                                     InstantiationException {
    this.front = (T) frontInstance.frontClass().getDeclaredConstructors()[0]
            .newInstance(frontInstance.args());
  }

  public static <T extends Front> Props props(FrontInstance<T> frontInstance) {
    return Props.create(FrontActor.class, frontInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(OnStart.class, hole -> front.onStart(item -> hole.consumer().tell(item, self())))
            .match(RequestNext.class, request -> front.onRequestNext(request.time()))
            .build();
  }
}
