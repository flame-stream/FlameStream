package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class IdentityJoba implements Joba {
  private final DataType generates;

  public IdentityJoba(DataType generates) {
    this.generates = generates;
  }

  @Override
  public DataType generates() {
    return generates;
  }

  @Override
  public int id() {
    return -1;
  }

  @Override
  public ActorRef materialize(ActorSystem at, ActorRef sink) {
    return sink;
  }
}
