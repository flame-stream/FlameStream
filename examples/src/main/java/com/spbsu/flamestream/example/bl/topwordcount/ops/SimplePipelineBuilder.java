package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  private class StatefulOpPipeline<Input, State, Output> {
    StatefulOp<Input, State, Output> op;
    Hashing<Input> hashing;

    StatefulOpPipeline(StatefulOp<Input, State, Output> op, Hashing<Input> hashing) {
      this.op = op;
      this.hashing = hashing;
    }

    private class Item {
      final Either<Input, State> value;
      final Input hash;

      Item(Either<Input, State> value, Input hash) {
        this.value = value;
        this.hash = hash;
      }

      @Override
      public int hashCode() {
        return hashing.hash(hash);
      }
    }

    private class HashedState {
      final State state;
      final Input hash;

      private HashedState(State state, Input hash) {
        this.state = state;
        this.hash = hash;
      }
    }

    private class Source implements SerializableFunction<Input, Stream<Item>> {
      @Override
      public Stream<Item> apply(Input input) {
        return Stream.of(new Item(new Left<>(input), input));
      }
    }

    private class Reducer implements SerializableFunction<List<Item>, Stream<HashedState>> {
      @Override
      public Stream<HashedState> apply(List<Item> items) {
        switch (items.size()) {
          case 1:
            final Item item = items.get(0);
            if (item.value.isLeft()) {
              return Stream.of(new HashedState(op.aggregate(item.value.left().get(), null), item.hash));
            }
            return Stream.empty();
          case 2:
            final Item right = items.get(0);
            final Item left = items.get(1);
            if (right.value.isRight() && left.value.isLeft()) {
              return Stream.of(new HashedState(
                      op.aggregate(left.value.left().get(), right.value.right().get()),
                      left.hash
              ));
            }
            return Stream.empty();
          default:
            throw new IllegalStateException("Group size should be 1 or 2");
        }
      }
    }

    private class Regrouper implements SerializableFunction<HashedState, Stream<Item>> {
      @Override
      public Stream<Item> apply(HashedState item) {
        return Stream.of(new Item(new Right<>(item.state), item.hash));
      }
    }

    private class Sink implements SerializableFunction<HashedState, Stream<Output>> {
      @Override
      public Stream<Output> apply(HashedState item) {
        return Stream.of(op.release(item.state));
      }
    }

    Node build(Graph.Builder graphBuilder) {
      final FlameMap<Input, Item> source = new FlameMap<>(new Source(), op.inputClass());
      final Grouping grouping = new Grouping<>(
              HashFunction.objectHash(Item.class),
              (o1, o2) -> {
                final Item payload = o1.payload(Item.class);
                final Item payload1 = o2.payload(Item.class);
                return hashing.equals(payload.hash, payload1.hash);
              },
              2,
              Item.class
      );
      final FlameMap<List<Item>, HashedState> reducer = new FlameMap<>(new Reducer(), List.class);
      final FlameMap<HashedState, Item> regrouper = new FlameMap<>(new Regrouper(), HashedState.class);
      final FlameMap<HashedState, Output> sink = new FlameMap<>(new Sink(), HashedState.class);
      graphBuilder
              .link(source, grouping)
              .link(grouping, reducer)
              .link(reducer, regrouper)
              .link(reducer, sink)
              .link(regrouper, grouping);

      ArrayList<Graph.Vertex> colocated = new ArrayList<Graph.Vertex>(Arrays.asList(
              grouping,
              reducer,
              regrouper,
              sink
      ));
      return new Node() {
        public Graph.Vertex source() {
          return source;
        }

        public Graph.Vertex sink() { return sink; }

        public void connect(Node node) {
          graphBuilder.link(sink, node.source());
          colocated.add(node.source());
        }

        public void build() {
          graphBuilder.colocate(colocated.toArray(new Graph.Vertex[]{}));
        }
      };
    }
  }

  public <Input, State, Output> Node node(StatefulOp<Input, State, Output> op, Hashing<Input> hashing) {
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
