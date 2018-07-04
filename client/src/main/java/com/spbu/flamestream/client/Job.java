package com.spbu.flamestream.client;

import com.spbsu.flamestream.core.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public interface Job {
  Graph graph();

  Stream<Front> fronts();

  Stream<Rear> rears();

  class Front {
    private final String id;
    private final String host;
    private final int port;

    public Front(String id, String host, int port) {
      this.id = id;
      this.host = host;
      this.port = port;
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
  }

  class Rear {
    private final String id;
    private final String host;
    private final int port;

    public Rear(String id, String host, int port) {
      this.id = id;
      this.host = host;
      this.port = port;
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
