package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.Condition;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.job.control.ConditionFails;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Artem on 12.11.2016.
 */
public class IndicatorJoba extends Joba.Stub {
  private Sink sink;
  private boolean stateIsOk;

  private final List<Condition> conditions = new ArrayList<>();
  public IndicatorJoba(Sink sink, Condition... conditions) {
    super(null);
    this.conditions.addAll(Arrays.asList(conditions));
    this.sink = sink;
  }

  @Override
  public void accept(DataItem item) {
    if (stateIsOk) {
      for (Condition c : conditions) {
        //noinspection unchecked
        stateIsOk = c.update(item.as(c.getClass().getGenericSuperclass().getClass()));
      }
      if (stateIsOk) {
        sink.accept(item);
      }
    }
  }

  @Override
  public void accept(Control control) {
    if (control instanceof EndOfTick) {
      if (stateIsOk) {
        sink.accept(control);
      } else {
        sink.accept(new ConditionFails());
      };
    } else {
      sink.accept(control);
    }
  }
}
