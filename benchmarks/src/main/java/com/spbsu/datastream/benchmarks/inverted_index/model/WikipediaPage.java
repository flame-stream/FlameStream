package com.spbsu.datastream.benchmarks.inverted_index.model;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WikipediaPage {
  private final int id;
  private final int version;
  private final String title;
  private final String text;

  public WikipediaPage(int id, int version, String title, String text) {
    this.id = id;
    this.version = version;
    this.title = title;
    this.text = text;
  }

  public int id() {
    return id;
  }

  public int version() {
    return version;
  }

  public String text() {
    return text;
  }

  public String title() {
    return title;
  }
}
