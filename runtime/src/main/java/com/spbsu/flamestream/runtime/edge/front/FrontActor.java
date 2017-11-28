package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor<T extends Front> extends LoggingActor {
  private final T front;
  private final String id;

  private FrontActor(FrontInstance<T> frontInstance) throws
                                                     IllegalAccessException,
                                                     InvocationTargetException,
                                                     InstantiationException {
    this.id = frontInstance.id();
    this.front = (T) frontInstance.front().getDeclaredConstructors()[0]
            .newInstance(frontInstance.params().toArray());
  }

  public static <T extends Front> Props props(FrontInstance<T> frontInstance) {
    return Props.create(FrontActor.class, frontInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }
}
