package experiments.artem.mockstream.impl;

import experiments.artem.mockstream.Condition;
import experiments.artem.mockstream.DataItem;

public class NotZeroCondition implements Condition {
  private static final Integer ZERO = 0;

  @Override
  public boolean update(DataItem item) {
    Integer data = item.data(Integer.class);
    return !ZERO.equals(data);
  }
}
