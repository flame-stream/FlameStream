package com.spbsu.datastream.example.usercounter;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
@UseHash(UserGrouping.class)
public class CountUserEntries implements Function<UserContainer[], UserCounter> {
  @Override
  public UserCounter apply(UserContainer[] containers) {
    final UserContainer first = containers[0];
    if (containers.length < 2)
      return new UserCounter((UserQuery) first);
    else {
      final UserContainer second = containers[1];
      if (first instanceof UserCounter && second instanceof UserQuery)
        return new UserCounter((UserCounter) first, (UserQuery) second);
      else
        return null;
    }
  }
}
