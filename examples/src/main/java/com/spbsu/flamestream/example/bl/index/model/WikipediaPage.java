package com.spbsu.flamestream.example.bl.index.model;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WikipediaPage {
  private final int id;
  private final int version;
  private final String title;
  private final String text;

  public WikipediaPage(int id,
                       int version,
                       String title,
                       String text) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WikipediaPage that = (WikipediaPage) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return id;
  }
}
