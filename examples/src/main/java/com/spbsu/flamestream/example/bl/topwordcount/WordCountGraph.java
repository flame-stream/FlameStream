package com.spbsu.flamestream.example.bl.topwordcount;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;
import com.spbsu.flamestream.example.bl.topwordcount.ops.CounterBuilder;
import com.spbsu.flamestream.example.bl.topwordcount.ops.TopBuilder;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class WordCountGraph implements Supplier<Graph> {
  @Override
  public Graph get() {
    final Source source = new Source();
    final Pattern pattern = Pattern.compile("\\s");
    final FlameMap<String, WordEntry> splitter = new FlameMap<>(s -> Arrays.stream(pattern.split(s))
            .map(WordEntry::new), String.class);
    final Sink sink = new Sink();
    final Graph.Builder graphBuilder = new Graph.Builder();
    final Graph.Vertex vertex = new CounterBuilder().build(graphBuilder, new TopBuilder(2).build(graphBuilder, sink));
    return graphBuilder
            .link(source, splitter)
            .link(splitter, vertex)
            .colocate(source, splitter, vertex)
            .build(source, sink);
  }
}
