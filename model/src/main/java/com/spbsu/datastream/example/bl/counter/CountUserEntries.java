package com.spbsu.datastream.example.bl.counter;

import com.spbsu.datastream.core.Filter;
import com.spbsu.datastream.example.bl.UseHash;
import com.spbsu.datastream.example.bl.UserContainer;
import com.spbsu.datastream.example.bl.UserGrouping;
import com.spbsu.datastream.example.bl.UserQuery;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
@UseHash(UserGrouping.class)
public class CountUserEntries implements Filter<UserContainer[], UserCounter> {
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

  @Override
  public boolean processOutputByElement() {
    return false;
  }
}
