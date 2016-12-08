package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.core.item.ObjectDataItem;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class FilterJoba extends Joba.Stub {
  private final Function func;
  private final Class blInput;
  private final Class blOutput;
  Sink sink;

  public FilterJoba(Sink sink, DataType generates, Function func, Class blInput, Class blOutput) {
    super(generates);
    this.func = func;
    this.blInput = blInput;
    this.blOutput = blOutput;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem item) {
    final Object result = func.apply(item.as(blInput));
    if (result != null)
      sink.accept(new ObjectDataItem(result, blOutput, item.meta()));
  }

  @Override
  public void accept(Control control) {
    sink.accept(control);
  }
}
