package com.spbsu.datastream.example.invertedindex;

/**
 * Author: Artem
 * Date: 17.01.2017
 */
public class WikiPage implements WikiPageContainer {
  private final int id;
  private final String title;
  private final String text;

  public WikiPage(int id, String title, String text) {
    this.id = id;
    this.title = title;
    this.text = text;
  }

  @Override
  public int pageId() {
    return id;
  }

  public String text() {
    return text;
  }

  public String title() {
    return title;
  }

  public String textAndTitle() {
    return title + "\n" + text;
  }
}
