package com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier;

import org.jetbrains.annotations.NotNull;

public class Topic implements Comparable<Topic> {
  private final String name;
  private final String id;
  private final double probability;

  Topic(String name, String id, double probability) {
    this.name = name;
    this.id = id;
    this.probability = probability;
  }

  @Override
  public int compareTo(@NotNull Topic o) {
    return Double.compare(o.probability(), probability);
  }

  public double probability() {
    return probability;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "Topic{" +
            "name='" + name + '\'' +
            ", id='" + id + '\'' +
            ", probability=" + probability +
            '}';
  }
}
