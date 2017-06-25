package com.spbsu.datastream.core.user_count;

import java.util.List;
import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 25.06.2017
 */
public class WrongOrderingFilter implements Predicate<List<UserContainer>> {
  @Override
  public boolean test(List<UserContainer> userContainers) {
    if (userContainers.size() > 2)
      throw new IllegalStateException("Group size should be <= 2");
    else if (userContainers.size() == 1 && !(userContainers.get(0) instanceof UserQuery))
      throw new IllegalStateException("The only element in group should be UserQuery");

    return userContainers.size() == 1 || (userContainers.get(0) instanceof UserCounter && userContainers.get(1) instanceof UserQuery);
  }
}
