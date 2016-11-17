package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.example.bl.UserContainer;
import com.spbsu.datastream.example.bl.UserQuery;

import java.util.function.Function;

/**
 * Created by Artem on 15.11.2016.
 */
public class UserFilterByName implements Function<UserContainer, UserContainer> {
  private String user;

  public UserFilterByName(String user) {
    this.user = user;
  }

  @Override
  public UserContainer apply(UserContainer userContainer) {
    if (userContainer instanceof UserQuery) {
      if (userContainer.user().equals(user)) {
        return userContainer;
      } else {
        return null;
      }
    } else {
      return userContainer;
    }
  }
}
