package com.spbsu.flamestream.runtime.edge.rear;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class RearActor<T extends Rear> extends LoggingActor {
  private final T rear;

  private RearActor(RearInstance<T> rearInstance) throws
                                                  IllegalAccessException,
                                                  InvocationTargetException,
                                                  InstantiationException {
    this.rear = (T) rearInstance.rear().getDeclaredConstructors()[0]
            .newInstance(rearInstance.args());
  }

  public static <T extends Rear> Props props(RearInstance<T> rearInstance) {
    return Props.create(RearActor.class, rearInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Object.class, rear::accept)
            .build();
  }
}
