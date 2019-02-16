package com.spbsu.flamestream.example.bl.tfidf.model;

public class TextDocument {
  private final String name;
  private final String content;
  private final String partitioning;
  private final int number;

  public TextDocument(String name, String content, String partitioning, int number) {
    this.name = name;
    this.content = content;
    this.partitioning = partitioning;
    this.number = number;
  }

  public String name() {
    return name;
  }

  public String content() {
    return content;
  }

  public String partitioning() {
    return partitioning;
  }

  public int number() {
    return number;
  }

  @Override
  public String toString() {
    return String.format("%s: >%s<", name, content);
  }
}
