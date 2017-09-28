package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSink;
import com.spbsu.flamestream.runtime.TheGraph;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public class FlamesStreamTestGraphs {
  public static TheGraph createTheGraph(FlameStreamExample flameStreamExample, Collection<Integer> fronts, AtomicGraph sink) {
    final BarrierSink barrierSink = new BarrierSink(sink);
    final Graph graph = flameStreamExample.graph(barrierSink);
    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> graph.inPorts().get(0)));
    return new TheGraph(graph, frontBindings);
  }
}
