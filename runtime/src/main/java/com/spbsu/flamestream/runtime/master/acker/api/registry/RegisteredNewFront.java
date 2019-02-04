package com.spbsu.flamestream.runtime.master.acker.api.registry;

import akka.actor.ActorRef;

public class RegisteredNewFront {
  private final FrontTicket frontTicket;
  private final ActorRef sender;

  public RegisteredNewFront(FrontTicket frontTicket, ActorRef sender) {
    this.frontTicket = frontTicket;
    this.sender = sender;
  }

  public FrontTicket frontTicket() {
    return this.frontTicket;
  }

  public ActorRef sender() {
    return this.sender;
  }
}

