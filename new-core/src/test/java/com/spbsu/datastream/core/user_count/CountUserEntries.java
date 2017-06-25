package com.spbsu.datastream.core.user_count;

import java.util.List;
import java.util.function.Function;

/**
 * User: Artem
 * Date: 25.06.2017
 */
public class CountUserEntries implements Function<List<UserContainer>, UserCounter> {

  @Override
  public UserCounter apply(List<UserContainer> userContainers) {
    if (userContainers.size() == 1) {
      return new UserCounter((UserQuery) userContainers.get(0));
    } else {
      return new UserCounter((UserCounter) userContainers.get(0));
    }
  }
}
