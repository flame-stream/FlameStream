package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.configuration.HashRange;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.*;

public final class RoutingInfo {
  private final Map<HashRange, ActorRef> rangeConcierges;
  private final ActorRef acker;

  public RoutingInfo(Map<HashRange, ActorRef> rangeConcierges, ActorRef acker) {
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
    return "RoutingInfo{" +
            "rangeConcierges=" + rangeConcierges +
            ", acker=" + acker +
            '}';
  }
}
