package com.spbsu.flamestream.runtime.edge.rear;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class RearActor extends LoggingActor {
  private final Rear rear;

  private RearActor(EdgeContext context, RearInstance<?> rearInstance) throws
                                                                       IllegalAccessException,
                                                                       InvocationTargetException,
                                                                       InstantiationException {
    this.rear = (Rear) rearInstance.clazz().getDeclaredConstructors()[0]
            .newInstance(context);
  }

  public static Props props(EdgeContext context, RearInstance<?> rearInstance) {
    return Props.create(RearActor.class, context, rearInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, rear::accept)
            .build();
  }
}
