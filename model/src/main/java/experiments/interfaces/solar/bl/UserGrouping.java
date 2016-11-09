package experiments.interfaces.solar.bl;

import experiments.interfaces.solar.DataItem;

import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserGrouping implements DataItem.Grouping {
  public long hash(DataItem item) {
    return item.as(UserContainer.class).user().hashCode();
  }

  @Override
  public boolean equals(DataItem left, DataItem right) {
    return left.as(UserContainer.class).user().equals(right.as(UserContainer.class).user());
  }
}
