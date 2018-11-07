package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimplePipelineBuilder {
  public interface Node {
    Graph.Vertex source();

    Graph.Vertex sink();

    void connect(Node node);

    void build();
  }

  private final Source source = new Source();
  private final Sink sink = new Sink();
  private final Graph.Builder graphBuilder = new Graph.Builder();
  private final ArrayList<Node> nodes = new ArrayList<>();

  private class StatefulOpPipeline<Input, Output extends Input> {
    StatefulOp<Input, Output> op;
    Hashing<Input> hashing;

    StatefulOpPipeline(StatefulOp<Input, Output> op, Hashing<Input> hashing) {
      this.op = op;
      this.hashing = hashing;
    }

    private class Item {
      final Input value;
      final boolean reduced;

      Item(Input value, boolean reduced) {
        this.value = value;
        this.reduced = reduced;
      }

      @Override
      public int hashCode() {
        return hashing.hash(value);
      }
    }

    private class Source implements Function<Input, Stream<Item>> {
      @Override
      public Stream<Item> apply(Input input) {
        return Stream.of(new Item(input, false));
      }
    }

    private class Reducer implements Function<List<Item>, Stream<Output>> {
      @Override
      public Stream<Output> apply(List<Item> items) {
        switch (items.size()) {
          case 1:
            final Item item = items.get(0);
            if (!item.reduced) {
              return Stream.of(op.output(item.value));
            }
            return Stream.empty();
          case 2:
            final Item right = items.get(0);
            final Item left = items.get(1);
            if (right.reduced && !left.reduced) {
              return Stream.of(op.reduce(op.output(left.value), op.output(right.value)));
            }
            return Stream.empty();
          default:
            throw new IllegalStateException("Group size should be 1 or 2");
        }
      }
    }

    private class Regrouper implements Function<Output, Stream<Item>> {
      @Override
      public Stream<Item> apply(Output item) {
        return Stream.of(new Item(item, true));
      }
    }

    Node build(Graph.Builder graphBuilder) {
      final FlameMap<Input, Item> source = new FlameMap<>(new Source(), op.inputClass());
      //noinspection Convert2Lambda
      final Grouping grouping = new Grouping<>(
              hashing.hashFunction(HashFunction.objectHash(Item.class)),
              hashing.equalz(new Equalz() {
                @Override
                public boolean test(DataItem o1, DataItem o2) {
                  final Item payload = o1.payload(Item.class);
                  final Item payload1 = o2.payload(Item.class);
                  return hashing.equals(payload.value, payload1.value);
                }
              }),
              2,
              Item.class
      );
      final FlameMap<List<Item>, Output> reducer = new FlameMap<>(new Reducer(), List.class);
      final FlameMap<Output, Item> regrouper = new FlameMap<>(new Regrouper(), op.outputClass());
      graphBuilder
              .link(source, grouping)
              .link(grouping, reducer)
              .link(reducer, regrouper)
              .link(regrouper, grouping);

      ArrayList<Graph.Vertex> colocated = new ArrayList<Graph.Vertex>(Arrays.asList(grouping, reducer, regrouper));
      return new Node() {
        public Graph.Vertex source() {
          return source;
        }

        public Graph.Vertex sink() { return reducer; }

        public void connect(Node node) {
          graphBuilder.link(reducer, node.source());
          colocated.add(node.source());
        }

        public void build() {
          graphBuilder.colocate(colocated.toArray(new Graph.Vertex[]{}));
        }
      };
    }
  }

  public <Input, Output extends Input> Node node(StatefulOp<Input, Output> op, Hashing<Input> hashing) {
    Node node = new StatefulOpPipeline<>(op, hashing).build(graphBuilder);
    nodes.add(node);
    return node;
  }

  public <Input, Output> Node node(MapOp<Input, Output> op) {
    Graph.Vertex vertex = new FlameMap<>(op, op.inputClass());
    final Node node = new Node() {
      public Graph.Vertex source() {
        return vertex;
      }

      public Graph.Vertex sink() { return vertex; }

      public void connect(Node node) {
        graphBuilder.link(vertex, node.source());
      }

      public void build() {}
    };
    nodes.add(node);
    return node;
  }

  public void connect(Node source, Node sink) {
    source.connect(sink);
  }

  public void connectToSource(Node node) {
    graphBuilder.link(source, node.source());
  }

  public void connectToSink(Node node) {
    graphBuilder.link(node.sink(), sink);
  }

  public Graph build() {
    nodes.forEach(Node::build);
    return graphBuilder.build(source, sink);
  }
}
