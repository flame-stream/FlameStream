package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class GroupedReducerBuilder<Input, Output extends Input> {
  private final Class<Input> inputClass;
  private final Class<Output> outputClass;

  GroupedReducerBuilder(Class<Input> inputClass, Class<Output> outputClass) {
    this.inputClass = inputClass;
    this.outputClass = outputClass;
  }

  private class Item {
    final Input value;
    final boolean reduced;

    Item(Input value, boolean reduced) {
      this.value = value;
      this.reduced = reduced;
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
            return Stream.of(output(item.value));
          }
          return Stream.empty();
        case 2:
          final Item right = items.get(0);
          final Item left = items.get(1);
          if (right.reduced && !left.reduced) {
            return Stream.of(reduce(output(left.value), output(right.value)));
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

  public Graph.Vertex build(Graph.Builder graphBuilder, Graph.Vertex to) {
    final FlameMap<Input, Item> source = new FlameMap<>(new Source(), inputClass);
    final Grouping grouping = new Grouping<>(groupingHashFunction(), groupingEqualz(), 2, Item.class);
    final FlameMap<List<Item>, Output> reducer = new FlameMap<>(new Reducer(), List.class);
    final FlameMap<Output, Item> regrouper = new FlameMap<>(new Regrouper(), outputClass);
    graphBuilder
            .link(source, grouping)
            .link(grouping, reducer)
            .link(reducer, to)
            .link(reducer, regrouper)
            .link(regrouper, grouping)
            .colocate(grouping, reducer, regrouper, to);
    return source;
  }

  abstract int groupingHash(Input input);

  @SuppressWarnings("Convert2Lambda")
  HashFunction groupingHashFunction() {
    return HashFunction.uniformHash(new HashFunction() {
      @Override
      public int hash(DataItem item) {
        final Item payload = item.payload(Item.class);
        return groupingHash(payload.value);
      }
    });
  }

  abstract boolean groupingEquals(Input left, Input right);

  @SuppressWarnings("Convert2Lambda")
  protected Equalz groupingEqualz() {
    return new Equalz() {
      @Override
      public boolean test(DataItem o1, DataItem o2) {
        final Item payload = o1.payload(Item.class);
        final Item payload1 = o2.payload(Item.class);
        return groupingEquals(payload.value, payload1.value);
      }
    };
  }

  abstract Output output(Input input);

  abstract Output reduce(Output left, Output right);
}
