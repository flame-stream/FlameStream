package com.spbsu.flamestream.runtime.node.tick.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.runtime.node.tick.range.HashRange;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class TickRoutes {
  private final Map<HashRange, ActorRef> rangeConcierges;
  private final ActorRef acker;

  public TickRoutes(Map<HashRange, ActorRef> rangeConcierges, ActorRef acker) {
    this.rangeConcierges = new HashMap<>(rangeConcierges);
    this.acker = acker;
  }

  public Map<HashRange, ActorRef> rangeConcierges() {
    return unmodifiableMap(rangeConcierges);
  }

  public ActorRef acker() {
    return acker;
  }

  @Override
  public String toString() {
    return "TickRoutes{" + "rangeConcierges=" + rangeConcierges + ", acker=" + acker + '}';
  }
}
