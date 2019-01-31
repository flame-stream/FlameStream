package com.spbsu.flamestream.example.bl.classifier;

class Topic {
    private final String name;
    private final String id;
    private final double probability;

    Topic(String name, String id, double probability) {
        this.name = name;
        this.id = id;
        this.probability = probability;
    }

    double getProbability() {
      return probability;
    }
}
