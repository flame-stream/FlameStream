package com.spbsu.flamestream.example.bl.classifier;

import org.jetbrains.annotations.NotNull;

class Topic implements Comparable<Topic> {
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

  double probability() {
    return probability;
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
