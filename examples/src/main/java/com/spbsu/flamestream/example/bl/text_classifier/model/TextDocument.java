package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

public class TextDocument implements DocContainer {
  private final String name;
  private final String content;
  private final String topic;
  private final String partitioning;
  private final int number;

  public TextDocument(String name, String content, String topic, String partitioning, int number) {
    this.name = name;
    this.content = content;
    this.topic = topic;
    this.partitioning = partitioning;
    this.number = number;
  }

  public String name() {
    return name;
  }

  public String content() {
    return content;
  }

  public String topic() {
    return topic;
  }

  @Override
  public String document() {
    return name;
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
