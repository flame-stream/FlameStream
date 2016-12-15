package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.Condition;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.control.ConditionTriggered;
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
  private final List<Condition> conditions = new ArrayList<>();
  private Condition triggeredCondition;

  public IndicatorJoba(Sink sink, DataType generates, Class blClass, Condition... conditions) {
    super(generates);
    this.sink = sink;
    this.blClass = blClass;
    for (Condition condition : conditions) {
      this.conditions.add((Condition) condition.create());
    }
  }

  @Override
  public void accept(DataItem item) {
    if (triggeredCondition == null) {
      for (Condition condition : conditions) {
        //noinspection unchecked
        boolean update = condition.update(item.as(blClass));
        if (!update) {
          triggeredCondition = condition;
          break;
        }
      }
      if (triggeredCondition == null) {
        sink.accept(item);
      }
    }
  }

  @Override
  public void accept(Control control) {
    if (control instanceof EndOfTick) {
      if (triggeredCondition == null) {
        sink.accept(control);
      } else if (triggeredCondition.isFinished()) {
        Output.instance().done();
        sink.accept(control);
      } else {
        sink.accept(new ConditionTriggered(triggeredCondition));
      }
    } else {
      sink.accept(control);
    }
  }
}