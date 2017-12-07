package com.spbsu.flamestream.runtime.edge.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.front.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.front.api.Start;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor extends LoggingActor {
  private final Front front;

  private FrontActor(EdgeContext context, AttachFront<?> attachFront) throws
                                                                          IllegalAccessException,
                                                                          InvocationTargetException,
                                                                          InstantiationException {
    this.front = (Front) attachFront.clazz().getDeclaredConstructors()[0]
            .newInstance(context);
  }

  public static Props props(EdgeContext context, AttachFront<?> attachFront) {
    return Props.create(FrontActor.class, context, attachFront);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Start.class, hole -> front.onStart(item -> hole.consumer().tell(item, self())))
            .match(RequestNext.class, request -> front.onRequestNext(request.time()))
            .build();
  }
}
