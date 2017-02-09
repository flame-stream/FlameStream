package com.spbsu.datastream.example.invertedindex.models;

/**
 * Author: Artem
 * Date: 17.01.2017
 */
public class WikiPage implements WordContainer {
  private int id;
  private String title;
  private String text;

  public WikiPage(int id, String title, String text) {
    this.id = id;
    this.title = title;
    this.text = text;
  }

  public int id() {
    return id;
  }

  public String text() {
    return text;
  }

  public String title() {
    return title;
  }

  @Override
  public String word() {
    return null;
  }

  public String titleAndText() {
    return title + "\n" + text;
  }
}
