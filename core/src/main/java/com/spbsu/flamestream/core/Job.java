package com.spbsu.flamestream.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface Job {
  Graph graph();

  Stream<Front> fronts();

  Stream<Rear> rears();

  class Front {
    private final String id;
    private final String host;
    private final int port;
    private final Class<?>[] inputClasses;

    public Front(String id, String host, int port, Class<?>... inputClasses) {
      this.id = id;
      this.host = host;
      this.port = port;
      this.inputClasses = Arrays.copyOf(inputClasses, inputClasses.length);
    }

    public String id() {
      return id;
    }

    public String host() {
      return this.host;
    }

    public int port() {
      return this.port;
    }

    public Stream<Class<?>> inputClasses() {
      return Arrays.stream(inputClasses);
    }
  }

  class Rear {
    private final String id;
    private final String host;
    private final int port;
    private final Class<?>[] outputClasses;

    public Rear(String id, String host, int port, Class<?>... outputClasses) {
      this.id = id;
      this.host = host;
      this.port = port;
      this.outputClasses = Arrays.copyOf(outputClasses, outputClasses.length);
    }

    public String id() {
      return id;
    }

    public String host() {
      return this.host;
    }

    public int port() {
      return this.port;
    }

    public Stream<Class<?>> outputClasses() {
      return Arrays.stream(outputClasses);
    }
  }

  class Builder {
    private final List<Front> fronts = new ArrayList<>();
    private final List<Rear> rears = new ArrayList<>();
    private final Graph graph;

    public Builder(Graph graph) {
      this.graph = graph;
    }

    public Builder addFront(Front front) {
      fronts.add(front);
      return this;
    }

    public Builder addRear(Rear rear) {
      rears.add(rear);
      return this;
    }

    public Job build() {
      return new Job() {
        @Override
        public Graph graph() {
          return graph;
        }

        @Override
        public Stream<Front> fronts() {
          return fronts.stream();
        }

        @Override
        public Stream<Rear> rears() {
          return rears.stream();
        }
      };
    }
  }
}
