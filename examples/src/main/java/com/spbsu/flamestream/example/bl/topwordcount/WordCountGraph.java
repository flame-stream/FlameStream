package com.spbsu.flamestream.example.bl.topwordcount;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;
import com.spbsu.flamestream.example.bl.topwordcount.ops.BucketedTopStatefulOp;
import com.spbsu.flamestream.example.bl.topwordcount.ops.CounterStatefulOp;
import com.spbsu.flamestream.example.bl.topwordcount.ops.Pipeline;
import com.spbsu.flamestream.example.bl.topwordcount.ops.SimplePipelineBuilder;
import com.spbsu.flamestream.example.bl.topwordcount.ops.TopTopStatefulOp;

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
    SimplePipelineBuilder simplePipelineBuilder = new SimplePipelineBuilder();
    simplePipelineBuilder.add(new CounterStatefulOp());
    simplePipelineBuilder.add(new BucketedTopStatefulOp(2, 2));
    simplePipelineBuilder.add(new TopTopStatefulOp(2));
    Pipeline pipeline = simplePipelineBuilder.build(graphBuilder);
    return graphBuilder
            .link(source, splitter)
            .link(splitter, pipeline.in())
            .link(pipeline.out(), sink)
            .colocate(source, splitter, pipeline.in())
            .build(source, sink);
  }
}
