package com.spbsu.datastream.core.job;

import com.spbsu.commons.util.Pair;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.condition.Condition;
import com.spbsu.datastream.core.condition.ConditionState;
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
  private final List<Pair<FailCondition, ConditionState>> failConditions = new ArrayList<>();
  private final List<Pair<DoneCondition, ConditionState>> doneConditions = new ArrayList<>();
  private boolean taskFail;
  private boolean taskDone;

  public IndicatorJoba(Sink sink, DataType generates, Class blClass, Condition... conditions) {
    super(generates);
    this.sink = sink;
    this.blClass = blClass;
    try {
      for (Condition condition : conditions) {
        if (condition instanceof FailCondition) {
          //noinspection unchecked
          failConditions.add(new Pair(condition, condition.conditionState().newInstance()));
        } else if (condition instanceof DoneCondition) {
          //noinspection unchecked
          doneConditions.add(new Pair(condition, condition.conditionState().newInstance()));
        }
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void accept(DataItem item) {
    if (!taskFail && !taskDone) {
      for (Pair<FailCondition, ConditionState> pair : failConditions) {
        //noinspection unchecked
        taskFail = pair.first.taskFail(item.as(blClass), pair.second);
      }
      if (!taskFail) {
        for (Pair<DoneCondition, ConditionState> pair : doneConditions) {
          //noinspection unchecked
          taskDone = pair.first.taskDone(item.as(blClass), pair.second);
        }
        sink.accept(item);
      }
    }
  }

  @Override
  public void accept(Control control) {
    if (control instanceof EndOfTick) {
      if (!taskFail) {
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