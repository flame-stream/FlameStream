package com.spbsu.datastream.example.invertedindex;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Author: Artem
 * Date: 31.01.2017
 */
public class WordOutput implements WikiPageContainer {
  @JsonProperty
  private final String word;
  @JsonProperty
  private final int pageId;
  @JsonProperty
  private final int position;
  @JsonProperty
  private final ActionType actionType;

  public WordOutput(String word, int pageId, int position, ActionType actionType) {
    this.word = word;
    this.pageId = pageId;
    this.position = position;
    this.actionType = actionType;
  }

  @Override
  public int pageId() {
    return pageId;
  }

  public int position() {
    return position;
  }

  public String word() {
    return word;
  }

  public ActionType outputType() {
    return actionType;
  }

  public enum ActionType {
    ADD, REMOVE
  }
}
