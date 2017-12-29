package com.spbsu.flamestream.runtime.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRear;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class RearActor extends LoggingActor {
  private final Rear rear;

  private RearActor(EdgeContext edgeContext, FlameRuntime.RearInstance<?> rearInstance) throws
          IllegalAccessException,
          InvocationTargetException,
          InstantiationException {
    final Object[] params;
    //handle AkkaRear in order to not create yet another actor system
    if (rearInstance.clazz().equals(AkkaRear.class)) {
      params = new Object[rearInstance.params().length + 2];
      params[0] = edgeContext;
      params[1] = context();
      System.arraycopy(rearInstance.params(), 0, params, 2, rearInstance.params().length);
    } else {
      params = new Object[rearInstance.params().length + 1];
      params[0] = edgeContext;
      System.arraycopy(rearInstance.params(), 0, params, 1, rearInstance.params().length);
    }
    rear = (Rear) rearInstance.clazz().getDeclaredConstructors()[0].newInstance(params);
  }

  public static Props props(EdgeContext edgeContext, FlameRuntime.RearInstance<?> rearInstance) {
    return Props.create(RearActor.class, edgeContext, rearInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, rear::accept)
            .build();
  }
}
