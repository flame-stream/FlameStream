package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import gnu.trove.list.TLongList;

/**
 * Created by Artem on 05.02.2017.
 */
public class WordOutput implements WordContainer {
  @JsonProperty
  private String word;
  @JsonProperty
  private ActionType actionType;
  private TLongList pagePositions;

  public WordOutput(String word, TLongList pagePositions, ActionType actionType) {
    this.word = word;
    this.pagePositions = pagePositions;
    this.actionType = actionType;
  }

  public String word() {
    return word;
  }

  @JsonProperty
  public long[] pagePositions() {
    return pagePositions.toArray();
  }

  public ActionType actionType() {
    return actionType;
  }

  public enum ActionType {
    ADD, REMOVE
  }
}