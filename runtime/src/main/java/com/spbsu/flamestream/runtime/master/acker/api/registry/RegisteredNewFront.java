package com.spbsu.flamestream.runtime.master.acker.api.registry;

import akka.actor.ActorRef;

public class RegisteredNewFront {
  public FrontTicket frontTicket;
  public ActorRef sender;

  public RegisteredNewFront(FrontTicket frontTicket, ActorRef sender) {
    this.frontTicket = frontTicket;
    this.sender = sender;
  }
}

