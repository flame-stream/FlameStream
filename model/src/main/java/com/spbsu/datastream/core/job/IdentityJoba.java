package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.Control;

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
  public void accept(DataItem item) {

  }

  @Override
  public void accept(Control control) {

  }
}
