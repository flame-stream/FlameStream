package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.condition.Condition;
import com.spbsu.datastream.core.condition.DoneCondition;
import com.spbsu.datastream.core.condition.FailCondition;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.control.ConditionFails;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Artem on 12.11.2016.
 */
public class IndicatorJoba extends Joba.Stub {
  private Sink sink;
  private final Class blClass;
  private final List<FailCondition> failConditions = new ArrayList<>();
  private final List<DoneCondition> doneConditions = new ArrayList<>();
  private boolean stateIsOk;
  private boolean taskDone;

  public IndicatorJoba(Sink sink, DataType generates, Class blClass, Condition... conditions) {
    super(generates);
    this.sink = sink;
    this.blClass = blClass;
    stateIsOk = true;
    for (Condition condition : conditions) {
      if (condition instanceof FailCondition) {
        failConditions.add((FailCondition) condition);
      } else if (condition instanceof DoneCondition) {
        doneConditions.add((DoneCondition) condition);
      }
    }
  }

  @Override
  public void accept(DataItem item) {
    if (stateIsOk && !taskDone) {
      for (FailCondition condition : failConditions) {
        //noinspection unchecked
        stateIsOk = condition.stateOk(item.as(blClass));
      }
      if (stateIsOk) {
        for (DoneCondition condition : doneConditions) {
          //noinspection unchecked
          taskDone = condition.taskDone(item.as(blClass));
        }
        sink.accept(item);
      }
    }
  }

  @Override
  public void accept(Control control) {
    if (control instanceof EndOfTick) {
      if (stateIsOk) {
        if (taskDone) {
          Output.instance().done();
        }
        sink.accept(control);
      }
      else {
        sink.accept(new ConditionFails());
      }
    } else {
      sink.accept(control);
    }
  }
}