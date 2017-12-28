package com.spbsu.flamestream.runtime.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRear;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class RearActor extends LoggingActor {
  private final Rear rear;

  private RearActor(EdgeId edgeId, FlameRuntime.RearInstance<?> rearInstance) throws
          IllegalAccessException,
          InvocationTargetException,
          InstantiationException {
    //handle AkkaRear in order to not create yet another actor system
    if (rearInstance.clazz().equals(AkkaRear.class)) {
      rear = (Rear) rearInstance.clazz().getDeclaredConstructors()[0].newInstance(edgeId, context());
    } else {
      rear = (Rear) rearInstance.clazz().getDeclaredConstructors()[0].newInstance(rearInstance.params());
    }
  }

  public static Props props(EdgeId edgeId, FlameRuntime.RearInstance<?> rearInstance) {
    return Props.create(RearActor.class, edgeId, rearInstance);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, rear::accept)
            .build();
  }
}
