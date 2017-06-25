package com.spbsu.datastream.core.user_count;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class UserQuery implements UserContainer {
  private final String user;

  UserQuery(String user) {
    this.user = user;
  }

  @Override
  public String user() {
    return user;
  }
}
