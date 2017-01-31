package com.spbsu.akka;

import akka.actor.ActorRef;

/**
 * @author vpdelta
 */
public interface MessageCapture {
  default void capture(ActorRef from, ActorRef to, Object message) {};

  static MessageCapture instance() {
    return Holder.capture;
  }

  class Holder {
    protected static MessageCapture capture = new MessageCapture() {};
  }
}
