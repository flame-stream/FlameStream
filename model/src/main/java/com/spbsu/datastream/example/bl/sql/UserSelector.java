package com.spbsu.datastream.example.bl.sql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.example.bl.UserContainer;
import com.spbsu.datastream.example.bl.UserQuery;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Artem on 15.11.2016.
 */
public class UserSelector implements UserContainer {
  @JsonProperty
  private String user;
  @JsonProperty
  private Queue<UserQuery> selectResult;

  public UserSelector() {}
  public UserSelector(UserQuery userQuery) {
    user = userQuery.user();
    selectResult = new LinkedBlockingQueue<>();
  }

  @SuppressWarnings("UnusedParameters")
  public UserSelector(UserSelector userSelector, UserQuery first) {
    user = userSelector.user();
    selectResult = userSelector.selectResult();
    selectResult.offer(first);
  }

  @Override
  public String user() {
    return user;
  }

  public Queue<UserQuery> selectResult() {
    return selectResult;
  }
}
