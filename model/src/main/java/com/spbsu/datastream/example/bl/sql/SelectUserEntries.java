package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.example.bl.*;

import java.util.function.Function;

/**
 * Created by Artem on 15.11.2016.
 */
@UseHash(UserGrouping.class)
public class SelectUserEntries implements Function<UserContainer[], UserSelector> {
  @Override
  public UserSelector apply(UserContainer[] containers) {
    final UserContainer first = containers[0];
    if (containers.length < 2)
      return new UserSelector((UserQuery) first);
    else {
      final UserContainer second = containers[1];
      if (first instanceof UserSelector && second instanceof UserQuery)
        return new UserSelector((UserSelector) first, (UserQuery) second);
      else
        return null;
    }
  }
}
