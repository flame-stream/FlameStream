package com.spbsu.datastream.core.user_count;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class UserCounter implements UserContainer {
  private final String user;
  private final int count;

  UserCounter(UserQuery userQuery) {
    this.user = userQuery.user();
    this.count = 1;
  }

  UserCounter(UserCounter userCounter) {
    this.user = userCounter.user;
    this.count = userCounter.count + 1;
  }

  @Override
  public String user() {
    return user;
  }

  int count() {
    return count;
  }
}
