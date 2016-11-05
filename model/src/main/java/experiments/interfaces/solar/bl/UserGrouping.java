package experiments.interfaces.solar.bl;

import experiments.interfaces.solar.DataItem;

import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserGrouping implements DataItem.Grouping {
  @Override
  public long hash(DataItem item) {
    return ((UserContainer) item.as(List.class).get(0)).user().hashCode();
  }

  @Override
  public boolean equals(DataItem left, DataItem right) {
    return ((UserContainer) left.as(List.class).get(0)).user().equals(((UserContainer) right.as(List.class).get(0)).user());
  }
}
