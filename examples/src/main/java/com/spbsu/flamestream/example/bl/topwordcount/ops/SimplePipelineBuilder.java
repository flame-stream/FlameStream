package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimplePipelineBuilder {
  private BiFunction<Graph.Builder, Graph.Vertex, Pipeline> builder = null;

  private class StatefulOpPipeline<Input, Output extends Input> {
    StatefulOp<Input, Output> op;

    StatefulOpPipeline(StatefulOp<Input, Output> op) {
      this.op = op;
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
        return op.groupingHash(value);
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

    Pipeline build(Graph.Builder graphBuilder, Graph.Vertex maybeTo) {
      final FlameMap<Input, Item> source = new FlameMap<>(new Source(), op.inputClass());
      final Grouping grouping = new Grouping<>(
              op.groupingHashFunction(HashFunction.objectHash(Item.class)),
              op.groupingEqualz(new Equalz() {
                @Override
                public boolean test(DataItem o1, DataItem o2) {
                  final Item payload = o1.payload(Item.class);
                  final Item payload1 = o2.payload(Item.class);
                  return op.groupingEquals(payload.value, payload1.value);
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

      if (maybeTo == null) {
        graphBuilder
                .colocate(grouping, reducer, regrouper);
      } else {
        graphBuilder
                .link(reducer, maybeTo)
                .colocate(grouping, reducer, regrouper, maybeTo);
      }
      return new Pipeline() {
        @Override
        public Graph.Vertex in() {
          return source;
        }

        @Override
        public Graph.Vertex out() {
          return maybeTo == null ? reducer : maybeTo;
        }
      };
    }
  }

  public <Input, Output extends Input> SimplePipelineBuilder add(StatefulOp<Input, Output> op) {
    BiFunction<Graph.Builder, Graph.Vertex, Pipeline> previousBuilder = builder;
    builder = (graphBuilder, nextBuilder) -> {
      Pipeline pipeline = new StatefulOpPipeline<Input, Output>(op).build(
              graphBuilder,
              nextBuilder
      );
      Graph.Vertex in =
              previousBuilder == null ? pipeline.in() : previousBuilder.apply(graphBuilder, pipeline.in()).in();
      return new Pipeline() {
        @Override
        public Graph.Vertex in() {
          return in;
        }

        @Override
        public Graph.Vertex out() {
          return pipeline.out();
        }
      };
    };
    return this;
  }

  public Pipeline build(Graph.Builder graphBuilder) {
    return builder.apply(graphBuilder, null);
  }
}
