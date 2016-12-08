package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.io.Output;

/**
 * Experts League
 * Created by solar on 03.11.16.
 */
public interface Joba extends Sink {
  DataType generates();
  int id();

  abstract class Stub implements Joba {
    private final DataType generates;
    private final int id;

    protected Stub(DataType generates) {
      this.generates = generates;
      this.id = Output.instance().registerJoba(this);
    }

    @Override
    public DataType generates() {
      return generates;
    }

    @Override
    public int id() {
      return id;
    }
  }
}
