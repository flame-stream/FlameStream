package com.spbsu.flamestream.example.inverted_index.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WikipediaPage {
  private final int id;
  private final int version;
  private final String title;
  private final String text;

  @JsonCreator
  public WikipediaPage(@JsonProperty("id") int id,
                       @JsonProperty("version") int version,
                       @JsonProperty("title") String title,
                       @JsonProperty("text") String text) {
    this.id = id;
    this.version = version;
    this.title = title;
    this.text = text;
  }

  @JsonProperty("id")
  public int id() {
    return id;
  }

  @JsonProperty("version")
  public int version() {
    return version;
  }

  @JsonProperty("text")
  public String text() {
    return text;
  }

  @JsonProperty("title")
  @SuppressWarnings("unused")
  public String title() {
    return title;
  }
}
