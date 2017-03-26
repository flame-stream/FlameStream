package com.spbsu.experiments.inverted_index.common_bl.models;

import com.spbsu.experiments.inverted_index.common_bl.utils.PageVersions;

/**
 * Author: Artem
 * Date: 17.01.2017
 */
public class WikiPage implements WordContainer {
  private final int id;
  private final int version;
  private final String title;
  private final String text;

  public WikiPage(int id, String title, String text) {
    this.id = id;
    this.title = title;
    this.text = text;
    version = PageVersions.updateVersion(id);
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
  public String word() {
    return null;
  }

  public String titleAndText() {
    return title + "\n" + text;
  }

  @Override
  public String toString() {
    return title;
  }
}
